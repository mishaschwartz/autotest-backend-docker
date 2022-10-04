import os
import io
import json
import time
import glob

import docker
import redis

import concurrent.futures
from contextlib import contextmanager
from typing import Optional, Dict

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REGISTRY_HOST = os.path.join(_, "/") if (_ := os.environ.get("REGISTRY_HOST")) else ""
_DOWNLOAD_IMAGE_NAME = "autotest-file-download"
_ENTRYPOINT_IMAGE_NAME = "autotest-entrypoint"
_TESTERS_DIR = os.environ.get("TESTERS_DIR", os.path.join(_THIS_DIR, "testers"))


def redis_connection(url: Optional[str] = os.environ.get("REDIS_URL"), **kwargs: Dict) -> redis.Redis:
    kwargs = {"decode_responses": True, **kwargs}
    return redis.Redis.from_url(url, **kwargs) if url else redis.Redis(**kwargs)


def loads_partial_json(json_string: str, expected_type: Optional[Type] = None) -> Tuple[List, bool]:
    """
    Return a list of objects loaded from a json string and a boolean
    indicating whether the json_string was malformed.  This will try
    to load as many valid objects as possible from a (potentially
    malformed) json string. If the optional expected_type keyword argument
    is not None then only objects of the given type are returned,
    if any objects of a different type are found, the string will
    be treated as malfomed.
    """
    i = 0
    decoder = json.JSONDecoder()
    results = []
    malformed = False
    json_string = json_string.strip()
    while i < len(json_string):
        try:
            obj, ind = decoder.raw_decode(json_string[i:])
            next_i = i + ind
            if expected_type is None or isinstance(obj, expected_type):
                results.append(obj)
            elif json_string[i:next_i].strip():
                malformed = True
            i = next_i
        except json.JSONDecodeError:
            if json_string[i].strip():
                malformed = True
            i += 1
    return results, malformed


def full_image_tag(local_tag):
    return f"{_REGISTRY_HOST}{local_tag}"


def _find_or_create_image(docker_client, image_name, dockerfile):
    tag = full_image_tag(image_name)
    try:
        return docker_client.images.get(tag)
    except docker.errors.ImageNotFound:
        pass
    if _REGISTRY_HOST:
        try:
            return docker_client.images.pull(tag)
        except docker.errors.NotFound:
            pass
    image = docker_client.images.build(path=_THIS_DIR, dockerfile=dockerfile, tag=tag, rm=True)
    if _REGISTRY_HOST:
        docker_client.images.push(tag)
    return image


def file_download_image(docker_client):
    return _find_or_create_image(docker_client, _DOWNLOAD_IMAGE_NAME, "file_download.Dockerfile")


def entrypoint_image(docker_client):
    return _find_or_create_image(docker_client, _ENTRYPOINT_IMAGE_NAME, "entrypoint.Dockerfile")


def download_files_to_volume(docker_client, file_url, credentials, volume_name):
    download_image = file_download_image(docker_client)
    docker.containers.run(
        download_image,
        remove=True,
        auto_remove=True,
        volumes=[f"{volume_name}:/files"],
        environment={"URL": file_url, "AUTH_TYPE": credentials["auth_type"], "CREDENTIALS": credentials["credentials"]},
    )


@contextmanager
def tmp_network(docker_client, *args, **kwargs):
    network = docker_client.network.create(*args, **kwargs)
    try:
        yield network
    finally:
        network.remove()


@contextmanager
def tmp_volume(
    docker_client,
    *args,
    driver=os.environ.get("VOLUME_DRIVER", "local"),
    driver_opts=json.loads(os.environ.get("VOLUME_DRIVER_OPTS").strip() or "{}"),
    **kwargs,
):
    volume = docker_client.volume.create(*args, driver=driver, driver_opts=driver_opts, **kwargs)
    try:
        yield volume
    finally:
        volume.remove()


def remove_all_containers(docker_client, container_suffixes, **kwargs):
    for suffix in container_suffixes:
        for container in docker_client.containers.list(all=True, filters={"name": suffix}):
            container.remove(**kwargs)


def get_feedback(container, test_data, test_id):
    feedback = []
    for file_path in test_data.get("feedback_file_names", []):
        abs_path = os.path.join("/workspace", file_path)
        try:
            file_data = container.get_archive(abs_path)
        except docker.errors.NotFound:
            raise Exception(f"Cannot find feedback file at '{file_path}'.")
        conn = redis_connection()
        id_ = conn.incr("autotest:feedback_files_id")
        key = f"autotest:feedback_file:{test_id}:{id_}"
        with tarfile.open(fileobj=BytesIO(b"".join(file_data))) as tf:
            member = tf.extractfile(abs_path)
            if member:
                conn.set(key, gzip.compress(member.read()))
                conn.expire(key, 3600)  # TODO: make this configurable
            else:
                raise Exception(f"Feedback file at '{file_path}' is not a regular file.")
        feedback.append(
            {
                "filename": feedback_file,
                "mime_type": mimetypes.guess_type(feedback_path)[0] or "text/plain",
                "compression": "gzip",
                "id": id_,
            }
        )
    return feedback


def get_result(container, test_data, test_id):
    stdout = container.logs(stderr=False, stdout=True)
    stderr = container.logs(stderr=True, stdout=False).strip()
    all_results, malformed = loads_partial_json(stdout, dict)

    state = container.client.api.inspect_container(container.name)["State"]
    start_time = dateutil.parser.isoparse(state["StartedAt"])
    end_time = dateutil.parser.isoparse(state["FinishedAt"])
    run_time = (end_time - start_time).total_seconds()

    result = {
        "time": run_time,
        "tests": [],
        "stderr": stderr or None,
        "malformed": stdout if malformed else None,
        "extra_info": test_data.get("extra_info", {}),
        "annotations": None,
        "feedback": get_feedback(container, test_data, test_id),
    }
    if run_time >= timeout:
        result["timeout"] = test_data["timeout"]
    for res in all_results:
        if "annotations" in res:
            result["annotations"] = res["annotations"]
        else:
            result["tests"].append(res)

    return result


def exec_test(test_data, image_name, id_suffix, script_volume_name, files_volume_name, test_env_vars):
    container_name = f"autotest-container-test-{id_suffix}"
    with tmp_network(docker_client, f"autotest-network-{id_suffix}") as network:
        container = docker_client.containers.run(
            image_name,
            detach=True,
            network=network,
            environment=test_env_vars,
            name=container_name,
            stop_signal=signal.SIGUSR1,
            volumes=[f"{script_volume_name}:/tmp/scripts:ro", f"{files_volume_name}:/tmp/files:ro"],
            command=json.dumps(test_data),
            entrypoint=["/entrypoint.sh"]
        )
        container.stop(timeout=test_data["timeout"])
    return (container,)  # TODO: add postgres, mongodb, etc. supporting containers to the network


def run_test(settings_id, test_id, files_url, categories, user, test_env_vars):
    results = []
    error = None
    try:
        settings = json.loads(redis_connection().hget("autotest:settings", key=settings_id))
        settings["_last_access"] = int(time.time())
        redis_connection().hset("autotest:settings", key=settings_id, value=json.dumps(settings))

        docker_client = docker.from_env()
        creds = json.loads(redis_connection().hget("autotest:user_credentials", key=user))
        files_volume_name = f"autotest-files-{settings_id}-{test_id}"
        with tmp_volume(name=files_volume_name):
            download_files_to_volume(docker_client, files_url, creds, files_volume_name)

            script_volume_name = settings["_scripts_volume"]
            container_suffixes = []
            results = []
            try:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = {}
                    for i, tester_settings in enumerate(test_settings["testers"]):
                        for j, test_data in enumerate(tester_settings["test_data"]):
                            if set(test_category) & set(categories):
                                id_suffix = f"{settings_id}-{test_id}-{i}-{j}"
                                container_suffixes.append(id_suffix)
                                image_name = tester_settings["_image"]
                                future = executor.submit(
                                    exec_test,
                                    test_data,
                                    image_name,
                                    id_suffix,
                                    script_volume_name,
                                    files_volume_name,
                                    test_env_vars,
                                )
                                futures[future] = (test_data, id_suffix)
                    for future in concurrent.futures.as_compoleted(futures):
                        tester_container, *_supporting_containers = future.result()
                        test_data, id_suffix = futures[future]
                        results.append(get_result(tester_container, test_data, test_id))
            finally:
                remove_all_containers(docker_client, container_suffixes, force=True)
    except Exception as e:
        error = str(e)
    finally:
        key = f"autotest:test_result:{test_id}"
        redis_connection().set(key, json.dumps({"test_groups": results, "error": error}))
        redis_connection().expire(key, 3600)  # TODO: make this configurable


def update_test_settings(user, settings_id, test_settings, file_url):
    try:
        creds = json.loads(redis_connection().hget("autotest:user_credentials", key=user))
        docker_client = docker.from_env()

        script_volume_name = f"autotest-files-{settings_id}"
        try:
            old_volume = docker_client.volumes.get(script_volume_name)
            # docker_client.containers.list(filters={'volume': script_volume_name))  # TODO: don't remove if in use
            old_volume.remove(force=True)
        except docker.errors.NotFound:
            pass
        docker_client.volumes.create(name=volume_name)
        download_files_to_volume(docker_client, file_url, creds, script_volume_name)
        test_settings["_scripts_volume"] = script_volume_name

        entrypoint_tag = entrypoint_image(docker_client).tags[0]

        for i, tester_settings in enumerate(test_settings["testers"]):
            tester_type = tester_settings["tester_type"]
            env_data = tester_settings.get("env_data", {})
            tag = full_image_tag(f"tester:{settings_id}.{i}")
            with open(os.path.join(_TESTERS_DIR, tester_type, 'Dockerfile')) as f:
                dockerfile_buffer = io.StringIO()
                dockerfile_buffer.write(f"{f.read()}\nCOPY --from={entrypoint_tag} --chmod=0744 /entrypoint.sh /entrypoint.sh\n")
                tester_image, _ = docker_client.images.build(
                    path=os.path.join(_TESTERS_DIR, tester_type),
                    fileobj=dockerfile_buffer,
                    tag=tag,
                    rm=True,
                    build_args={
                        "VERSION": env_data.get("version"),
                        "REQUIREMENTS": env_data.get("requirements"),
                    },
                )
            if _REGISTRY_HOST:
                docker_client.images.push(tag)
            tester_settings["_image"] = tester_image.tags[0]
            test_settings.pop("_error", None)
    except Exception as e:
        test_settings["_error"] = str(e)
        raise
    finally:
        test_settings["_user"] = user
        test_settings["_last_access"] = int(time.time())
        redis_connection().hset("autotest:settings", key=settings_id, value=json.dumps(test_settings))
