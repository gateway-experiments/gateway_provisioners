# Docker and Docker Swarm deployments

Because Gateway Provisioners is a _library package_ and not an _application_, deployment into Docker and Docker Swarm
configurations consists of ensuring that the _host application image_ has the appropriate kernel specifications in place
and that necessary configuration items (typically environment variables in containerized deployments) are
present in the host application.

With respect to Docker vs. Docker Swarm, Docker Swarm is more _service- and cluster-oriented_ and provides a better element of
_scheduling_ across a set of hosts, whereas Docker is just about containers running locally on the same host.

```{tip}
The following provides information for the kinds of tasks that should be considered when deploying applications
that use Gateway Provisioners on Docker.  See our [_Application Support_ page](https://github.com/jupyter-server/gateway_provisioners/tree/main/gateway_provisioners/app-support/README.md)
for examples of how to configure and deploy such applications.
```

## Generating Kernel Specifications

Kernelspec generation for Docker and Docker Swarm deployments is performed using the `jupyter-docker-spec` command.  Because
the host application will also reside within a docker image, the commands are usually placed into a Dockerfile
that _extends_ an existing image.  However, some may choose to `docker exec` into a running container, perform and test
the necessary configuration, then use `docker commit` to generate a new image.  That said, the following will assume a
Dockerfile approach.

```{attention}
For Docker Swarm deployments, be sure to include the `--swarm` flag.  This adds the appropriate provisioner reference
to the generated `kernel.json` file.
```

To generate a default kernel specification (where Python is the default kernel) enter:

```dockerfile
RUN jupyter docker-spec install
```

which produces the following output...

```text
[I 2023-02-15 14:10:16.892 DockerSpecInstaller] Installing kernel specification for 'Docker Python'
[I 2023-02-15 14:10:17.306 DockerSpecInstaller] Installed kernelspec docker_python in /usr/local/share/jupyter/kernels/docker_python
```

and the following set of files and directories:

```text
/usr/local/share/jupyter/kernels/docker_python
kernel.json logo-64x64.png

/usr/local/share/jupyter/kernels/docker_python/scripts:
launch_docker.py
```

where each provides the following function:

- `kernel.json` - the primary file that the host application uses to discover a given kernel's availability.
  This file contains _stanzas_ that describe the kernel's argument vector (`argv`), its runtime environment (`env`),
  its display name (`display_name`) and language (`language`), as
  well as its kernel provisioner's configuration (`metadata.kernel_provisioner`) - which, in this case, will reflect the
  `DockerProvisioner`.
- `logo-64x64.png` - the icon resource corresponding to this kernel specification.  Icon resource files must be start
  with the `logo-` prefix to be included in the kernel specification.
- `scripts/launch_docker.py` - the "launcher" for the kernel image identified by the
  `metadata.kernel_provisioner.config.image_name` entry.  This file can be modified to include instructions for
  volume mounts, etc., and is compatible with both Docker and Docker Swarm - performing the applicable instructions for
  each environment.

```{seealso}
See [Command-line Options](#command-line-options) below for how to adjust the `image-name`, `display-name`, and
others.
```

### Generating Multiple Specifications

Its common practice to support multiple languages or use different images for kernels of the same language.  For each
of those differences, a separate installation command should be provided:

```dockerfile
RUN jupyter docker-spec install --image-name my-numpy-image:dev --kernel-name my_numpy_kernel_py --display-name "My Numpy"
RUN jupyter docker-spec install --image-name my-tensor-image:dev --kernel-name my_tensor_kernel_py --display-name "My Tensorflow"
RUN jupyter docker-spec install --image-name my-R-image:dev --language R --display-name "My R Kernel"
```

### Docker and Docker Swarm Kernel Instances

Gateway Provisioners currently supports the launching of _vanilla_ (i.e., non-spark) kernels within a Docker Swarm cluster.
When kernels are launched, Gateway Provisioners is responsible for creating the appropriate entity. The kind of
entity created is a function of the corresponding kernel provisioner class.

When the kernel provisioner class is `DockerSwarmProvisioner` the `launch_docker.py` script will create a Docker Swarm
_service_. This service uses a restart policy of `none` meaning that it's configured to go away upon failures or
completion. In addition, because the kernel is launched as a swarm service, the kernel can "land" on any node of the cluster.

When the kernel provisioner class is `DockerProvisioner` the `launch_docker.py` script will create a traditional docker
_container_. As a result, the kernel will always reside on the same host as the corresponding host application.

Items worth noting:

1. The Swarm service or Docker container name will be composed of the launching username (`KERNEL_USERNAME`) and kernel-id.
1. The service/container will have 3 labels applied: `"kernel_id=<kernel-id>"`, `"component=kernel"`, and `"app=gateway-provisioners"` - similar to Kubernetes.
1. The service/container will be launched within the same docker network as the host application.

## Other Configuration Items

There are some environment variables that can be set in the host application's environment that affect how Gateway
Provisioners operate within a Docker and Docker Swarm environment.  For example, `GP_MIRROR_WORKING_DIRS` can be set
to `True`, instructing Gateway Provisioners to set the launched container's working directory to the value of
`KERNEL_WORKING_DIR`.  When this environment variable is enabled, it usually implies that volume mounts are in play
such that the per-user volumes are then available to the launched container.

Other [environment variables](config-add-env.md#additional-environment-variables) applicable to Docker/Docker Swarm
configurations are `GP_DOCKER_NETWORK` and `GP_PROHIBITED_UIDS`.

````{seealso}
```{eval-rst}
See :ref:`configuring-gp`, with a focus on docker-specific options, for
additional configuration options within the host application.
```
````

## Command-line Options

The following is produced using `jupyter docker-spec install --help` and displays the complete set of command-line
options:

```text
Creates a Jupyter kernel specification for use within a Docker or Docker Swarm
cluster.

Options
=======
The options below are convenience aliases to configurable class-options,
as listed in the "Equivalent to" description-line of the aliases.
To see all configurable class-options for some <cmd>, use:
    <cmd> --help-all

--swarm
    Install kernel for use within a Docker Swarm cluster.
    Equivalent to: [--DockerSpecInstaller.swarm=True]
--user
    Install to the per-user kernel registry
    Equivalent to: [--BaseSpecApp.user=True]
--sys-prefix
    Install to Python's sys.prefix. Useful in conda/virtual environments.
    Equivalent to: [--BaseSpecApp.prefix=/opt/miniconda3/envs/provisioners]
--debug
    set log level to logging.DEBUG (maximize logging output)
    Equivalent to: [--Application.log_level=10]
--image-name=<Unicode>
    The kernel image to use for this kernel specification. If this specification
    is enabled for Spark usage, this image will be the driver image.
    (GP_IMAGE_NAME env var)
    Default: None
    Equivalent to: [--DockerSpecInstaller.image_name]
--prefix=<Unicode>
    Specify a prefix to install to, e.g. an env. The kernelspec will be
    installed in PREFIX/share/jupyter/kernels/
    Default: ''
    Equivalent to: [--BaseSpecApp.prefix]
--kernel-name=<Unicode>
    Install the kernel spec into a directory with this name.
    Default: ''
    Equivalent to: [--BaseSpecApp.kernel_name]
--display-name=<Unicode>
    The display name of the kernel - used by user-facing applications.
    Default: ''
    Equivalent to: [--BaseSpecApp.display_name]
--language=<Unicode>
    The language of the kernel referenced in the kernel specification.  Must be one of
        'Python', 'R', or 'Scala'.  Default = 'Python'.
    Default: 'Python'
    Equivalent to: [--BaseSpecApp.language]
--authorized-users=<set-item-1>...
    List of user names against which KERNEL_USERNAME will be compared. Any match
    (case-sensitive) will allow the kernel's launch, otherwise an HTTP 403
    (Forbidden) error will be raised.  The set of unauthorized users takes
    precedence. This option should be used carefully as it can dramatically
    limit who can launch kernels. To specify multiple names via the CLI,
    separate options must be provided for each entry. (GP_AUTHORIZED_USERS env
    var - non-bracketed, just comma-separated)
    Default: set()
    Equivalent to: [--BaseSpecApp.authorized_users]
--unauthorized-users=<set-item-1>...
    List of user names against which KERNEL_USERNAME will be compared. Any match
    (case-sensitive) will prevent the kernel's launch and result in an HTTP 403
    (Forbidden) error. To specify multiple names via the CLI, separate options
    must be provided for each entry. (GP_UNAUTHORIZED_USERS env var - non-
    bracketed, just comma-separated)
    Default: {'root'}
    Equivalent to: [--BaseSpecApp.unauthorized_users]
--port-range=<Unicode>
    Specifies the lower and upper port numbers from which ports are created. The
    bounded values are separated by '..' (e.g., 33245..34245 specifies a range
    of 1000 ports to be randomly selected). A range of zero (e.g., 33245..33245
    or 0..0) disables port-range enforcement.  (GP_PORT_RANGE env var)
    Default: '0..0'
    Equivalent to: [--BaseSpecApp.port_range]
--launch-timeout=<Int>
    Number of ports to try if the specified port is not available
    (GP_LAUNCH_TIMEOUT env var)
    Default: 30
    Equivalent to: [--BaseSpecApp.launch_timeout]
--ipykernel-subclass-name=<Unicode>
    For Python kernels, the name of the ipykernel subclass.
    Default: 'ipykernel.ipkernel.IPythonKernel'
    Equivalent to: [--BaseSpecApp.ipykernel_subclass_name]
--log-level=<Enum>
    Set the log level by value or name.
    Choices: any of [0, 10, 20, 30, 40, 50, 'DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL']
    Default: 30
    Equivalent to: [--Application.log_level]
--config=<Unicode>
    Full path of a config file.
    Default: ''
    Equivalent to: [--JupyterApp.config_file]

Examples
--------

    jupyter-docker-spec install --language=R --kernel-name=r_docker --image-name=foo/my_r_kernel_image:v4_0

    jupyter-docker-spec install --swarm --kernel-name=python_swarm

To see all available configurables, use `--help-all`.
```
