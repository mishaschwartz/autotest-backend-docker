""" Autotest Backend: docker version """
import gzip
import mimetypes
import os
import io
import json
import tarfile
import time
import subprocess

import docker
import redis
import requests.exceptions
import rq

import concurrent.futures

from contextlib import contextmanager
from typing import Optional, Dict, Type, Tuple, List, Iterable, Union
from dateutil.parser import isoparse

from docker import errors as docker_errors
from docker.models.images import Image
from docker.models.networks import Network
from docker.models.volumes import Volume
from docker.models.containers import Container
from docker.errors import APIError as DockerApiError

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REGISTRY_URL = os.environ.get("REGISTRY_URL", "")
_DOWNLOAD_IMAGE_NAME = "autotest-file-download"

def redis_connection() -> redis.Redis:
    """
    Return a connection to a redis database.
    """
    return rq.get_current_job().connection


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


def full_image_tag(local_tag: str) -> str:
    """
    Return the local_tag prepended with the registry host if the REGISTRY_URL
    environment variable is set.
    """
    return f"{_REGISTRY_URL.rstrip('/')}/{local_tag}" if _REGISTRY_URL else local_tag


def _find_or_create_image(
    docker_client: docker.DockerClient, image_name: str, path: str = _THIS_DIR, **kwargs
) -> Image:
    """
    Return an image with name image_name if it exists, otherwise create it first
    based on kwargs and then return it.
    """
    tag = full_image_tag(image_name)
    try:
        return docker_client.images.get(tag)
    except docker_errors.ImageNotFound:
        pass
    if _REGISTRY_URL:
        try:
            return docker_client.images.pull(tag)
        except docker_errors.NotFound:
            pass
    image, _logs = docker_client.images.build(path=path, tag=tag, rm=True, **kwargs)
    if _REGISTRY_URL:
        docker_client.images.push(tag)
    return image


def file_download_image(docker_client: docker.DockerClient) -> Image:
    """
    Return an image used to download test files from a client's API.
    """
    return _find_or_create_image(docker_client, _DOWNLOAD_IMAGE_NAME, dockerfile="file_download.Dockerfile")


def plugin_image(docker_client: docker.DockerClient, name: str, path: str) -> Image:
    """
    Return an image for a plugin found at path.
    """
    return _find_or_create_image(docker_client, name, path=path)


def download_files_to_volume(
    docker_client: docker.DockerClient, file_url: str, credentials: Dict[str, str], volume_name: str
) -> None:
    """
    Downloads files from the file_url using the credentials in credentials and stores the
    unzipped files to a volume named volume_name.
    """
    download_image = file_download_image(docker_client)
    docker_client.containers.run(
        download_image,
        remove=True,
        volumes=[f"{volume_name}:/files"],
        environment={"URL": file_url, "AUTH_TYPE": credentials["auth_type"], "CREDENTIALS": credentials["credentials"]},
    )


@contextmanager
def tmp_network(docker_client: docker.DockerClient, *args, **kwargs) -> Iterable[Network]:
    """
    Yields a docker network created by passing args and kwargs to docker_client.networks.create
    and removes the network when the context is exited.
    """
    network = docker_client.networks.create(*args, **kwargs)
    try:
        yield network
    finally:
        for container in network.containers:
            network.disconnect(container, force=True)
        network.remove()


@contextmanager
def tmp_volume(
    docker_client: docker.DockerClient,
    *args,
    driver: str = os.environ.get("VOLUME_DRIVER", "local"),
    driver_opts: Dict = json.loads(os.environ.get("VOLUME_DRIVER_OPTS", "").strip() or "{}"),
    **kwargs,
) -> Iterable[Volume]:
    """
    Yields a docker volume created by passing args and kwargs to docker_client.volumes.create
    and removes the volume when the context is exited.
    """
    volume = docker_client.volumes.create(*args, driver=driver, driver_opts=driver_opts, **kwargs)
    try:
        yield volume
    finally:
        volume.remove()


def remove_all_containers(docker_client: docker.DockerClient, container_suffixes: List[str], **kwargs) -> None:
    """
    Remove all containers whose name contains one of the strings in container_suffixes. kwargs are passed to the
    `Container.remove` method.

    This is used to clean up all containers created by running a test in case they haven't already be removed.
    """
    for suffix in container_suffixes:
        for container in docker_client.containers.list(all=True, filters={"name": suffix}):
            container.remove(**kwargs)


def get_feedback(container: Container, test_data: Dict, test_id: Union[str, int]) -> List[Dict]:
    """
    Copy all feedback files specified in test_data out of container and save their content to the
    redis database.

    Return a list of dictionaries, each containing information about the content of each feedback
    file and how to retrieve its content later from the redis database.
    """
    feedback = []
    for file_path in test_data.get("feedback_file_names", []):
        abs_path = os.path.join("/workspace", file_path)
        try:
            file_data = container.get_archive(abs_path)
        except docker_errors.NotFound:
            raise Exception(f"Cannot find feedback file at '{file_path}'.")
        conn = redis_connection()
        id_ = conn.incr("autotest:feedback_files_id")
        key = f"autotest:feedback_file:{test_id}:{id_}"
        with tarfile.open(fileobj=io.BytesIO(b"".join(file_data))) as tf:
            member = tf.extractfile(abs_path)
            if member:
                conn.set(key, gzip.compress(member.read()))
                conn.expire(key, 3600)  # TODO: make this configurable
            else:
                raise Exception(f"Feedback file at '{file_path}' is not a regular file.")
        feedback.append(
            {
                "filename": os.path.basename(file_path),
                "mime_type": mimetypes.guess_type(file_path)[0] or "text/plain",
                "compression": "gzip",
                "id": id_,
            }
        )
    return feedback


def get_result(container: Container, test_data: Dict, test_id: Union[str, int]) -> Dict:
    """
    Return a dictionary containing the results of running the tests in container.
    Also copies any feedback files to the redis database.
    """
    stdout = container.logs(stderr=False, stdout=True).decode()
    stderr = container.logs(stderr=True, stdout=False).strip().decode()
    all_results, malformed = loads_partial_json(stdout, dict)

    state = container.client.api.inspect_container(container.name)["State"]
    start_time = isoparse(state["StartedAt"])
    end_time = isoparse(state["FinishedAt"])
    run_time = int((end_time - start_time).total_seconds() * 1000)

    result = {
        "time": run_time,
        "tests": [],
        "stderr": stderr or None,
        "malformed": stdout if malformed else None,
        "extra_info": test_data.get("extra_info", {}),
        "annotations": None,
        "feedback": get_feedback(container, test_data, test_id),
    }
    if run_time >= test_data["timeout"]:
        result["timeout"] = test_data["timeout"]
    for res in all_results:
        if "annotations" in res:
            result["annotations"] = res["annotations"]
        else:
            result["tests"].append(res)

    return result


def create_plugin_containers(
    docker_client: docker.DockerClient, plugin_data: Dict, network: Network, id_suffix: str
) -> Tuple[List[Container], Dict[str, str]]:
    """
    Create all plugin containers specified by plugin_data in the specified network and return a list of those
    containers and a dictionary of environment variables to be used to run the tester container.

    The environment variables are used to specify how the plugin containers should be accessed/used.
    """
    environment = {}
    plugin_containers = []
    for name, data in plugin_data.items():
        if data.get("enabled"):
            path = redis_connection().get(f"autotest:plugin:{name}")
            if path is None:
                raise Exception(f"plugin {name} is not installed")
            path = path.decode()
            cli = os.path.join(path, "docker.cli")
            stringified_data = {k: str(v) for k, v in data.items() if k != "enabled"}
            proc = subprocess.run(
                [cli, "before_test"], capture_output=True, universal_newlines=True, env=stringified_data
            )
            environment.update(json.loads(proc.stdout))
            plugin_image_tag = plugin_image(docker_client, name, path).tags[0]
            plugin_containers.append(
                docker_client.containers.run(
                    plugin_image_tag, name=f"autotest-container-test-{name}-{id_suffix}", detach=True, network=network
                )
            )
    return plugin_containers, environment


def exec_test(
    test_data: Dict,
    image_name: str,
    id_suffix: str,
    script_volume_name: str,
    files_volume_name: str,
    test_env_vars: Dict[str, str],
    plugin_data: Dict,
    volume_data: List[str],
    timeout: Optional[int],
    docker_client: docker.DockerClient,
) -> Tuple[Container, ...]:
    """
    Create all containers required to run the test as specified by the test_data and plugin_data. The tester
    container is created from the image with tag image_name.

    Use the id_suffix to name all networks all containers so that they can be identified later for removal.

    Mount all volumes specified by the script_volume_name, files_volume_name, and volume_data to the
    tester container.

    Return all containers created.
    """
    container_name = f"autotest-container-test-{id_suffix}"
    with tmp_network(docker_client, f"autotest-network-{id_suffix}") as network:
        plugin_containers, plugin_environment = create_plugin_containers(docker_client, plugin_data, network, id_suffix)
        environment = {"AUTOTESTENV": "true", **test_env_vars, **plugin_environment}
        data_volumes = [f"{name}:/data/{name}:ro" for name in volume_data]
        volumes = [f"{script_volume_name}:/tmp/scripts:ro", f"{files_volume_name}:/tmp/files:ro", *data_volumes]
        container = docker_client.containers.run(
            image_name,
            detach=True,
            network=network.name,
            environment=environment,
            name=container_name,
            volumes=volumes,
            command=[json.dumps(test_data)],
            entrypoint=["/entrypoint.sh"],
        )

        try:
            container.wait(timeout=timeout)
        except requests.exceptions.ReadTimeout:
            pass  # timeout is reached
        for container_ in [container, *plugin_containers]:
            try:
                container_.kill()
            except DockerApiError:
                pass  # container_ is already dead
    return container, *plugin_containers


def run_test(
    settings_id: Union[str, int],
    test_id: Union[str, int],
    files_url: str,
    categories: List[str],
    user: str,
    test_env_vars: Dict[str, str],
) -> None:
    """
    Run all tests by downloading the files at files_url and running the tests specified by the settings with id
    settings_id against those files. Only run tests that share a category with categories.

    Store the results of the test in the redis database using the test_id to identify the results.

    The user argument is used to retrieve credentials necessary to authorize the download of the files at files_url.

    The test_env_vars are additional environment variables to be passed to the process running the tests.
    """
    results = []
    error = None
    try:
        settings = json.loads(redis_connection().hget("autotest:settings", key=settings_id))
        settings["_last_access"] = int(time.time())
        redis_connection().hset("autotest:settings", key=settings_id, value=json.dumps(settings))

        docker_client = docker.from_env()
        creds = json.loads(redis_connection().hget("autotest:user_credentials", key=user))
        files_volume_name = f"autotest-files-{settings_id}-{test_id}"
        with tmp_volume(docker_client, name=files_volume_name):
            download_files_to_volume(docker_client, files_url, creds, files_volume_name)

            script_volume_name = settings["_scripts_volume"]
            container_suffixes = []
            results = []
            try:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = {}
                    for i, tester_settings in enumerate(settings["testers"]):
                        for j, test_data in enumerate(tester_settings["test_data"]):
                            plugin_data = test_data.get("plugins", {})
                            volume_data = test_data.get("data_entries", [])
                            if set(test_data["categories"]) & set(categories):
                                id_suffix = f"{settings_id}-{test_id}-{i}-{j}"
                                container_suffixes.append(id_suffix)
                                image_name = tester_settings["_image"]
                                future = executor.submit(
                                    exec_test,
                                    {"test_data": test_data},
                                    image_name,
                                    id_suffix,
                                    script_volume_name,
                                    files_volume_name,
                                    test_env_vars,
                                    plugin_data,
                                    volume_data,
                                    test_data["timeout"],
                                    docker_client,
                                )
                                futures[future] = (test_data, id_suffix)
                    for future in concurrent.futures.as_completed(futures):
                        tester_container, *supporting_containers = future.result()
                        test_data, id_suffix = futures[future]
                        results.append(get_result(tester_container, test_data, test_id))
            finally:
                remove_all_containers(docker_client, container_suffixes, force=True)
    except Exception as e:
        import traceback
        error = traceback.format_exc()
    finally:
        key = f"autotest:test_result:{test_id}"
        redis_connection().set(key, json.dumps({"test_groups": results, "error": error}))
        redis_connection().expire(key, 3600)  # TODO: make this configurable


def update_test_settings(user: str, settings_id: Union[str, int], test_settings: Dict, file_url: str) -> None:
    """
    Create images and volumes that will be used later to run tests.

    An image is created for each tester specified in the test_settings and a volume is created
    containing the files downloaded from file_url.

    Information about the image and volume are stored to the redis database using the settings_id to
    identify the settings.

    The user argument is used to retrieve credentials necessary to authorize the download of the files at file_url.
    """
    try:
        creds = json.loads(redis_connection().hget("autotest:user_credentials", key=user))
        docker_client = docker.from_env()

        script_volume_name = f"autotest-files-{settings_id}"
        try:
            old_volume = docker_client.volumes.get(script_volume_name)
            old_volume.remove(force=True)  # TODO: don't remove if in use
        except docker_errors.NotFound:
            pass
        docker_client.volumes.create(name=script_volume_name)
        download_files_to_volume(docker_client, file_url, creds, script_volume_name)
        test_settings["_scripts_volume"] = script_volume_name

        for i, tester_settings in enumerate(test_settings["testers"]):
            tester_type = tester_settings["tester_type"]
            env_data = tester_settings.get("env_data", {})
            tag = full_image_tag(f"autotest-tester:{settings_id}.{i}")
            tester_path = redis_connection().get(f"autotest:tester:{tester_type}")
            if tester_path is None:
                raise Exception(f"tester {tester_type} is not installed.")
            tester_path = tester_path.decode()
            tester_image, _ = docker_client.images.build(
                path=tester_path,
                tag=tag,
                rm=True,
                buildargs={
                    "VERSION": str(env_data.get("version")),
                    "REQUIREMENTS": env_data.get("requirements"),
                },
            )
            if _REGISTRY_URL:
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
