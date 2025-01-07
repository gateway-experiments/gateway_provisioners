# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
import os

from jupyter_core.application import JupyterApp
from overrides import overrides
from traitlets import Bool, Unicode, default

from .._version import __version__
from .base_app import DEFAULT_LANGUAGE, PYTHON, SCALA, BaseSpecApp, R

DEFAULT_KERNEL_NAMES = {PYTHON: "docker_python", SCALA: "docker_scala", R: "docker_r"}
KERNEL_SPEC_TEMPLATE_NAMES = {
    PYTHON: "container_python",
    SCALA: "container_scala",
    R: "container_r",
}
DEFAULT_DISPLAY_NAMES = {PYTHON: "Docker Python", SCALA: "Docker Scala", R: "Docker R"}
DEFAULT_IMAGE_NAMES = {
    PYTHON: "elyra/kernel-py",
    SCALA: "elyra/kernel-scala",
    R: "elyra/kernel-r",
}

DOCKER_PROVISIONER_NAME = "docker-provisioner"
SWARM_PROVISIONER_NAME = "docker-swarm-provisioner"
LAUNCHER_NAME = "launch_docker.py"


class DockerSpecInstaller(BaseSpecApp):
    """CLI for extension management."""

    name = "jupyter-docker-spec"
    description = (
        "Creates a Jupyter kernel specification for use within a Docker or Docker Swarm cluster."
    )
    # Note that the left justification of the second example is necessary to ensure proper
    # alignment with the first example during --help output.
    examples = """
    jupyter-docker-spec install --language=R --kernel-name=r_docker --image-name=foo/my_r_kernel_image:v4_0

jupyter-docker-spec install --swarm --kernel-name=python_swarm
    """

    @default("kernel_name")
    def _kernel_name_default(self) -> str:
        return DEFAULT_KERNEL_NAMES[DEFAULT_LANGUAGE]

    @default("display_name")
    def _display_name_default(self) -> str:
        return DEFAULT_DISPLAY_NAMES[DEFAULT_LANGUAGE]

    # Image name
    image_name_env = "GP_IMAGE_NAME"
    image_name = Unicode(
        None,
        config=True,
        allow_none=True,
        help="""The kernel image to use for this kernel specification. If this specification is
enabled for Spark usage, this image will be the driver image. (GP_IMAGE_NAME env var)""",
    )

    @default("image_name")
    def _image_name_default(self):
        return os.getenv(self.image_name_env)

    # Flags
    swarm = Bool(
        False, config=True, help="Install kernelspec for use within a Docker Swarm cluster."
    )

    provisioner_name = Unicode(DOCKER_PROVISIONER_NAME, config=False)
    launcher_name = Unicode(LAUNCHER_NAME, config=False)

    aliases = {
        "image-name": "DockerSpecInstaller.image_name",
    }
    aliases.update(BaseSpecApp.aliases)

    flags = {
        "swarm": (
            {"DockerSpecInstaller": {"swarm": True}},
            swarm.help,
        ),
    }
    flags.update(BaseSpecApp.flags)

    @overrides
    def detect_missing_extras(self):
        super().detect_missing_extras()
        try:
            import docker  # type:ignore[import-untyped] # noqa: F401
        except ImportError:
            self.log.warning(
                "The extra package 'docker' is not installed in this environment and is required.  "
                "Ensure that gateway_provisioners is installed by specifying the extra 'docker' "
                "(e.g., pip install 'gateway_provisioners[docker]')."
            )

    @overrides
    def validate_parameters(self):
        super().validate_parameters()

        self.language = self.language.lower()
        self.launcher_dir_name = "docker"
        self.resource_dir_name = self.language

        self.provisioner_name = SWARM_PROVISIONER_NAME if self.swarm else DOCKER_PROVISIONER_NAME

        if self.kernel_name == DEFAULT_KERNEL_NAMES[DEFAULT_LANGUAGE]:
            self.kernel_name = DEFAULT_KERNEL_NAMES[self.language]
        if self.display_name == DEFAULT_DISPLAY_NAMES[DEFAULT_LANGUAGE]:
            self.display_name = DEFAULT_DISPLAY_NAMES[self.language]

        self.kernel_spec_dir_name = KERNEL_SPEC_TEMPLATE_NAMES[self.language]

        if self.image_name is None:
            self.image_name = f"{DEFAULT_IMAGE_NAMES[self.language]}:{self._get_tag()}"

        # sanitize kernel_name
        self.kernel_name = self.kernel_name.replace(" ", "_")

    @overrides
    def get_substitutions(self, install_dir) -> dict:
        substitutions = super().get_substitutions(install_dir)
        substitutions["image_name"] = self.image_name
        substitutions["provisioner_name"] = self.provisioner_name
        substitutions["launcher_name"] = self.launcher_name
        return substitutions


class DockerProvisionerApp(JupyterApp):
    """Application responsible for driving the creation of Docker-based kernel specifications."""

    version = __version__
    name = "jupyter docker-spec"
    description = """Application used to create kernel specifications for use on Docker or Docker Swarm clusters
    via the DockerProvisioner or DockerSwarmProvisioner kernel provisioners."""
    subcommands = {
        "install": (
            DockerSpecInstaller,
            DockerSpecInstaller.description.splitlines()[0],
        ),
    }
    aliases = {}
    flags = {}

    def start(self):
        super().start()

        if self.subapp is None:
            print(f"No subcommand specified. Must specify one of: {list(self.subcommands)}")
            print()
            self.print_description()
            self.print_subcommands()
            self.exit(1)


if __name__ == "__main__":
    DockerProvisionerApp.launch_instance()
