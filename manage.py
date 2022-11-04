#!/usr/bin/env python3

"""
CLI to install the backend and manage plugins and data entries
"""

import sys
import os
import json
import subprocess
import argparse
import docker
import redis
from autotest_backend import plugin_image
from typing import Dict, Callable, Sequence, Iterable, Tuple
from docker import errors as docker_errors

REDIS_CONNECTION = redis.Redis.from_url(os.environ.get("REDIS_URL"), decode_responses=True)


def _schema() -> Dict:
    """
    Return a dictionary representation of the json loaded from the schema_skeleton.json file or from the redis
    database if it exists.
    """
    schema = REDIS_CONNECTION.get("autotest:schema")

    if schema:
        return json.loads(schema)

    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "schema_skeleton.json")) as f:
        return json.load(f)


def _print(*args_, **kwargs) -> None:
    """
    Exactly the same as the builtin print function but prepends "[AUTOTESTER]"
    """
    print("[AUTOTESTER]", *args_, **kwargs)


def parse_args() -> Callable:
    """
    Parses command line arguments using the argparse module and returns a function to call to
    execute the requested command.
    """
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="manager")

    subparsers.add_parser("tester", help="testers", description="testers")
    subparsers.add_parser("plugin", help="plugins", description="plugins")
    subparsers.add_parser("data", help="data", description="data")

    for name, parser_ in subparsers.choices.items():
        subsubparser = parser_.add_subparsers(dest="action")

        install_parser = subsubparser.add_parser("install", help=f"install {parser_.description}")

        if name == "data":
            install_parser.add_argument("name", help="name of a volume to register as a data volume")
        else:
            install_parser.add_argument("paths", nargs="+")

        remove_parser = subsubparser.add_parser("remove", help=f"remove {parser_.description}")
        remove_parser.add_argument("names", nargs="+")

        subsubparser.add_parser("list", help=f"list {parser_.description}")

        subsubparser.add_parser("clean", help=f"remove {parser_.description} that have been deleted on disk.")

    subparsers.add_parser("install", help="install backend")

    managers = {"install": BackendManager, "tester": TesterManager, "plugin": PluginManager, "data": DataManager}

    args = parser.parse_args()

    if args.manager == "install":
        args.action = "install"

    return getattr(managers[args.manager](args), args.action)


class _Manager:
    """
    Abstract Manager class used to manage resources
    """

    args: argparse.Namespace

    def __init__(self, args):
        self.args = args


class PluginManager(_Manager):
    """
    Manger for plugins
    """

    def install(self) -> None:
        """
        Install a plugin
        """
        schema = _schema()
        docker_client = docker.from_env()
        for path in self.args.paths:
            cli = os.path.join(path, "docker.cli")
            if os.path.isfile(cli):
                proc = subprocess.run([cli, "settings"], capture_output=True, check=False, universal_newlines=True)
                if proc.returncode:
                    _print(
                        f"Plugin settings could not be retrieved from plugin at {path}. Failed with:\n{proc.stderr}",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
                settings = json.loads(proc.stdout)
                plugin_name = list(settings.keys())[0]
                installed_plugins = schema["definitions"]["plugins"]["properties"]
                if plugin_name in installed_plugins:
                    _print(f"A plugin named {plugin_name} is already installed", file=sys.stderr, flush=True)
                    continue
                plugin_image(docker_client, plugin_name, path)
                installed_plugins.update(settings)
                REDIS_CONNECTION.set(f"autotest:plugin:{plugin_name}", path)
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    def remove(self, additional: Sequence = tuple()) -> None:
        """
        Remove a plugin
        """
        schema = _schema()

        installed_plugins = schema["definitions"]["plugins"]["properties"]
        for name in self.args.names + list(additional):
            REDIS_CONNECTION.delete(f"autotest:plugin:{name}")
            if name in installed_plugins:
                installed_plugins.remove(name)
            try:
                installed_plugins.pop(name)
            except KeyError:
                continue
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    @staticmethod
    def _get_installed() -> Iterable[Tuple[str, str]]:
        """
        Yield the name and path of all installed plugins
        """
        for plugin_key in REDIS_CONNECTION.keys("autotest:tuple:*"):
            plugin_name = plugin_key.split(":")[-1]
            path = REDIS_CONNECTION.get(plugin_key)
            yield plugin_name, path

    def list(self) -> None:
        """
        Print the name and path of all installed plugins
        """
        for plugin_name, path in self._get_installed():
            print(f"{plugin_name} @ {path}")

    def clean(self) -> None:
        """
        Remove all plugins that are installed but whose data has been removed from disk
        """
        to_remove = [plugin_name for plugin_name, path in self._get_installed() if not os.path.isdir(path)]
        _print("Removing the following testers:", *to_remove, sep="\t\n")
        self.remove(additional=to_remove)


class TesterManager(_Manager):
    """
    Manager for testers
    """

    def install(self) -> None:
        """
        Install testers
        """
        schema = _schema()
        for path in self.args.paths:
            cli = os.path.join(path, "docker.cli")
            if os.path.isfile(cli):
                proc = subprocess.run([cli, "settings"], capture_output=True, check=False, universal_newlines=True)
                if proc.returncode:
                    _print(
                        f"Tester settings could not be retrieved from tester at {path}. Failed with:\n{proc.stderr}",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
                settings = json.loads(proc.stdout)
                tester_name = settings["properties"]["tester_type"]["const"]
                installed_testers = schema["definitions"]["installed_testers"]["enum"]
                if tester_name in installed_testers:
                    _print(f"A tester named {tester_name} is already installed", file=sys.stderr, flush=True)
                    continue
                installed_testers.append(tester_name)
                schema["definitions"]["tester_schemas"]["oneOf"].append(settings)
                REDIS_CONNECTION.set(f"autotest:tester:{tester_name}", path)
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    def remove(self, additional: Sequence = tuple()) -> None:
        """
        Removes installed testers specified in self.args. Additional testers to remove can be specified
        with the additional keyword
        """
        schema = _schema()

        tester_settings = schema["definitions"]["tester_schemas"]["oneOf"]
        installed_testers = schema["definitions"]["installed_testers"]["enum"]
        for name in self.args.names + list(additional):
            REDIS_CONNECTION.delete(f"autotest:tester:{name}")
            if name in installed_testers:
                installed_testers.remove(name)
            for i, settings in enumerate(tester_settings):
                if name in settings["properties"]["tester_type"]["enum"]:
                    tester_settings.pop(i)
                    break
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    @staticmethod
    def _get_installed() -> Iterable[Tuple[str, str]]:
        """
        Yield the name and path of all installed testers
        """
        for tester_key in REDIS_CONNECTION.keys("autotest:tester:*"):
            tester_name = tester_key.split(":")[-1]
            path = REDIS_CONNECTION.get(tester_key)
            yield tester_name, path

    def list(self) -> None:
        """
        Print the name and path of all installed testers
        """
        for tester_name, path in self._get_installed():
            print(f"{tester_name} @ {path}")

    def clean(self) -> None:
        """
        Remove all testers that are installed but whose data has been removed from disk
        """
        to_remove = [tester_name for tester_name, path in self._get_installed() if not os.path.isdir(path)]
        _print("Removing the following testers:", *to_remove, sep="\t\n")
        self.remove(additional=to_remove)


class DataManager(_Manager):
    """
    Manager for data entries
    """

    def install(self) -> None:
        """
        Install a data entry
        """
        schema = _schema()

        installed_volumes = schema["definitions"]["data_entries"]["items"]["enum"]
        name = self.args.name

        if name in installed_volumes:
            _print(f"A volume named {name} is already installed", file=sys.stderr, flush=True)
            return

        try:
            docker.from_env().volumes.get(name)
        except docker_errors.NotFound:
            _print(f"No volume named {name} exists, please create it and try again", file=sys.stderr, flush=True)
            return

        installed_volumes.append(name)
        REDIS_CONNECTION.sadd("autotest:data_entries", name)
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    def remove(self, additional: Sequence = tuple()) -> None:
        """
        Removes installed data entries specified in self.args. Additional entries to remove can be specified
        with the additional keyword
        """
        schema = _schema()
        installed_volumes = schema["definitions"]["data_entries"]["items"]["enum"]
        for name in self.args.names + list(additional):
            installed_volumes.remove(name)
            REDIS_CONNECTION.srem("autotest:data_entries", name)
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    @staticmethod
    def list() -> None:
        """
        Print the name and path of all installed entries
        """
        print(*REDIS_CONNECTION.smembers("autotest:data_entries"), sep="\n")

    def clean(self) -> None:
        """
        Remove all data entries that are installed but whose data has been removed from disk
        """
        all_volumes = {v.name for v in docker.from_env().volumes.list()}
        to_remove = [name for name in REDIS_CONNECTION.smembers("autotest:data_entries") if name not in all_volumes]
        _print("Removing the following data entries:", *to_remove, sep="\t\n")
        self.remove(additional=to_remove)


class BackendManager(_Manager):
    """
    Manager for the autotest backend
    """

    @staticmethod
    def install() -> None:
        """
        Check that the backend can connect to the docker engine.
        """
        _print("checking docker connection")
        docker.from_env().ping()
        REDIS_CONNECTION.set("autotest:schema", json.dumps(_schema()))


if __name__ == "__main__":
    parse_args()()
