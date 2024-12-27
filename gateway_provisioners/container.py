# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
"""Code related to managing kernels running in containers."""
from __future__ import annotations

import os
import signal
from abc import abstractmethod
from typing import Any

import urllib3  # docker ends up using this and it causes lots of noise, so turn off warnings
from jupyter_client import localinterfaces
from overrides import overrides
from traitlets import Unicode

from .remote_provisioner import RemoteProvisionerBase

urllib3.disable_warnings()

local_ip = localinterfaces.public_ips()[0]

default_kernel_uid = "1000"  # jovyan user is the default
default_kernel_gid = "100"  # users group is the default

# These could be enforced via a PodSecurityPolicy, but those affect
# all pods so the cluster admin would need to configure those for
# all applications.
prohibited_uids = os.getenv("GP_PROHIBITED_UIDS", "0").split(",")
prohibited_gids = os.getenv("GP_PROHIBITED_GIDS", "0").split(",")

mirror_working_dirs = bool(os.getenv("GP_MIRROR_WORKING_DIRS", "false").lower() == "true")


class ContainerProvisionerBase(RemoteProvisionerBase):
    """Kernel provisioner for container-based kernels."""

    image_name_env = "GP_IMAGE_NAME"
    image_name = Unicode(
        None,
        config=True,
        allow_none=True,
        help="""The image name to use when launching container-based kernels.
(GP_IMAGE_NAME env var)""",
    )

    executor_image_name_env = "GP_EXECUTOR_IMAGE_NAME"
    executor_image_name = Unicode(
        None,
        config=True,
        allow_none=True,
        help="""The image name to use as the Spark executor image when launching
container-based kernels within Spark environments. (GP_EXECUTOR_IMAGE_NAME env var)""",
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.image_name = os.getenv(self.image_name_env) or self.image_name
        self.executor_image_name = (
            os.getenv(self.executor_image_name_env) or self.executor_image_name or self.image_name
        )
        self.container_name = None
        self.assigned_node_ip = None

    @property
    @overrides
    def has_process(self) -> bool:
        return self.container_name is not None

    @overrides
    async def pre_launch(self, **kwargs: Any) -> dict[str, Any]:
        # Unset assigned_host, ip, and node_ip in pre-launch, otherwise, these screw up restarts
        self.assigned_host = ""
        self.assigned_ip = None
        self.assigned_node_ip = None

        kwargs = await super().pre_launch(**kwargs)

        kwargs["env"]["KERNEL_IMAGE"] = self.image_name
        kwargs["env"]["KERNEL_EXECUTOR_IMAGE"] = self.executor_image_name

        if (
            not mirror_working_dirs
        ):  # If mirroring is not enabled, remove working directory if present
            if "KERNEL_WORKING_DIR" in kwargs["env"]:
                del kwargs["env"]["KERNEL_WORKING_DIR"]

        self._enforce_prohibited_ids(**kwargs)
        return kwargs

    @overrides
    def log_kernel_launch(self, cmd: list[str]) -> None:
        self.log.info(
            f"{self.__class__.__name__}: kernel launched. Kernel image: {self.image_name}, "
            f"KernelID: {self.kernel_id}, cmd: '{cmd}'"
        )

    def _enforce_prohibited_ids(self, **kwargs):
        """Determine UID and GID with which to launch container and ensure they are not prohibited."""
        kernel_uid = kwargs["env"].get("KERNEL_UID", default_kernel_uid)
        kernel_gid = kwargs["env"].get("KERNEL_GID", default_kernel_gid)

        if kernel_uid in prohibited_uids:
            error_message = (
                f"Kernel's UID value of '{kernel_uid}' has been denied via GP_PROHIBITED_UIDS!"
            )
            self.log_and_raise(PermissionError(error_message))
        elif kernel_gid in prohibited_gids:
            error_message = (
                f"Kernel's GID value of '{kernel_gid}' has been denied via GP_PROHIBITED_GIDS!"
            )
            self.log_and_raise(PermissionError(error_message))

        # Ensure the kernel's env has what it needs in case they came from defaults
        kwargs["env"]["KERNEL_UID"] = kernel_uid
        kwargs["env"]["KERNEL_GID"] = kernel_gid

    @overrides
    async def poll(self) -> int | None:
        """Determines if container is still active.

        Submitting a new kernel to the container manager will take a while to be Running.
        Thus, kernel ID will probably not be available immediately for poll.
        So will regard the container as active when no status is available or one of the initial
        phases.
        Returns None if the container cannot be found or its in an initial state. Otherwise,
        return an exit code of 0.
        """
        result: int | None = 0

        container_status = self.get_container_status(None)
        # Do not check whether container_status is None
        # EG couldn't restart kernels although connections exists.
        # See https://github.com/jupyter/enterprise_gateway/issues/827
        if container_status in self.get_initial_states():
            result = None

        return result

    @overrides
    async def send_signal(self, signum: int) -> None:
        """Send signal `signum` to container."""
        if signum == 0:
            await self.poll()
        elif signum == signal.SIGKILL:
            await self.kill()
        else:
            # This is very likely an interrupt signal, so defer to the super class
            # which should use the communication port.
            await super().send_signal(signum)

    @overrides
    async def kill(self, restart: bool = False) -> None:
        """Kills a containerized kernel."""

        if self.container_name:  # We only have something to terminate if we have a name
            self.terminate_container_resources(restart=restart)

    @overrides
    async def terminate(self, restart: bool = False) -> None:
        """Terminates a containerized kernel.

        This method defers to kill() since there's no distinction between the
        two in these environments.
        """
        await self.kill(restart=restart)

    @overrides
    async def shutdown_listener(self, restart: bool) -> None:
        await super().shutdown_listener(restart)
        if self.container_name:  # We only have something to terminate if we have a name
            self.terminate_container_resources(restart)

    @overrides
    async def confirm_remote_startup(self):
        """Confirms the container has started and returned necessary connection information."""
        self.log.debug("Trying to confirm kernel container startup status")
        self.start_time = RemoteProvisionerBase.get_current_time()
        i = 0
        ready_to_connect = False  # we're ready to connect when we have a connection file to use
        while not ready_to_connect:
            i += 1
            await self.handle_launch_timeout()

            container_status = self.get_container_status(str(i))
            if container_status:
                if container_status in self.get_error_states():
                    reason = f"Error starting kernel container; status: '{container_status}'.  Check server logs."
                    self.log_and_raise(RuntimeError(reason))
                else:
                    if self.assigned_host != "":
                        ready_to_connect = await self.receive_connection_info()
                        self.pid = (
                            0  # We won't send the process signals from container-based provisioners
                        )
                        self.pgid = 0
            else:
                self.detect_launch_failure()

    @overrides
    async def get_provisioner_info(self) -> dict[str, Any]:
        """Captures the base information necessary for kernel persistence relative to containers."""
        provisioner_info = await super().get_provisioner_info()
        provisioner_info.update(
            {
                "assigned_node_ip": self.assigned_node_ip,
            }
        )
        return provisioner_info

    @overrides
    async def load_provisioner_info(self, provisioner_info: dict) -> None:
        """Loads the base information necessary for kernel persistence relative to containers."""
        await super().load_provisioner_info(provisioner_info)
        self.assigned_node_ip = provisioner_info.get("assigned_node_ip")

    @abstractmethod
    def get_initial_states(self) -> set[str]:
        """Return list of states (in lowercase) indicating container is starting (includes running)."""
        raise NotImplementedError

    @abstractmethod
    def get_error_states(self) -> set[str]:
        """Returns the list of error states (in lowercase)."""
        raise NotImplementedError

    @abstractmethod
    def get_container_status(self, iteration: str | None) -> str:
        """Return current container state."""
        raise NotImplementedError

    @abstractmethod
    def terminate_container_resources(self, restart: bool = False) -> bool | None:
        """Terminate any artifacts created on behalf of the container's lifetime."""
        raise NotImplementedError
