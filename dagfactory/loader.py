import importlib

import yaml
from kubernetes.client import models as k8s


class KubernetesConstructor:

    @staticmethod
    def resource_requirements_constructor(loader, node):
        data = loader.construct_mapping(node, deep=True)
        return k8s.V1ResourceRequirements(**data)

    @staticmethod
    def container_constructor(loader, node):
        data = loader.construct_mapping(node, deep=True)
        if "resources" in data and data["resources"] is not None:
            if isinstance(data["resources"], dict):
                resource_node = yaml.nodes.MappingNode(
                    tag="!kubernetes.client.models.V1ResourceRequirements", value=list(data["resources"].items())
                )
                data["resources"] = KubernetesConstructor.resource_requirements_constructor(loader, resource_node)
        if "securityContext" in data and data["securityContext"] is not None:
            data["security_context"] = k8s.V1PodSecurityContext(**data.pop("securityContext"))

        return k8s.V1Container(**data)

    @staticmethod
    def pod_spec_constructor(loader, node):
        data = loader.construct_mapping(node, deep=True)

        if "containers" in data and data["containers"] is not None:
            converted_containers = []
            for container_data in data["containers"]:
                if isinstance(container_data, dict):
                    container_node = yaml.nodes.MappingNode(
                        tag="!kubernetes.client.models.V1Container", value=list(container_data.items())
                    )
                    converted_containers.append(KubernetesConstructor.container_constructor(loader, container_node))
                else:
                    converted_containers.append(container_data)
            data["containers"] = converted_containers
        if (
            "securityContext" in data
            and data["securityContext"] is not None
            and isinstance(data["securityContext"], dict)
        ):
            data["security_context"] = k8s.V1PodSecurityContext(**data.pop("securityContext"))

        return k8s.V1PodSpec(**data)

    @staticmethod
    def pod_constructor(loader, node):
        data = loader.construct_mapping(node, deep=True)

        if "spec" in data and data["spec"] is not None:
            if isinstance(data["spec"], dict):
                spec_node = yaml.nodes.MappingNode(
                    tag="!kubernetes.client.models.V1PodSpec", value=list(data["spec"].items())
                )
                data["spec"] = KubernetesConstructor.pod_spec_constructor(loader, spec_node)

        return k8s.V1Pod(**data)


def _dynamic_object_constructor(loader, node):
    tag = node.tag.strip("")
    class_path = node.tag.split(":")[2]
    parts = class_path.split(".")
    module_name = ".".join(parts[:-1])
    class_name = parts[-1]

    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise yaml.constructor.ConstructorError(f"Could not import module '{module_name}' for tag '{tag}': {e}")
    try:
        cls = getattr(module, class_name)
    except AttributeError:
        raise yaml.constructor.ConstructorError(
            f"Class '{class_name}' not found in module '{module_name}' for tag '{tag}'"
        )

    if isinstance(node, yaml.MappingNode):
        data = loader.construct_mapping(node, deep=True)
    elif isinstance(node, yaml.SequenceNode):
        data = loader.construct_sequence(node, deep=True)
    elif isinstance(node, yaml.ScalarNode):
        data = loader.construct_scalar(node)
    else:
        raise yaml.constructor.ConstructorError(f"Unsupported YAML node type for dynamic constructor: {type(node)}")

    if isinstance(data, dict):
        return cls(**data)
    elif isinstance(data, list) and issubclass(cls, list):
        return cls(data)
    elif isinstance(data, str) and issubclass(cls, str):
        return cls(data)
    else:
        return cls(data)


class DAGFactoryLoader(yaml.FullLoader):
    pass


DAGFactoryLoader.add_constructor(
    "!!kubernetes.client.models.V1ResourceRequirements", KubernetesConstructor.resource_requirements_constructor
)
DAGFactoryLoader.add_constructor("!!kubernetes.client.models.V1Container", KubernetesConstructor.container_constructor)
DAGFactoryLoader.add_constructor("!!kubernetes.client.models.V1PodSpec", KubernetesConstructor.pod_spec_constructor)
DAGFactoryLoader.add_constructor("!!kubernetes.client.models.V1Pod", KubernetesConstructor.pod_constructor)
# Handle unknown tag dynamically
DAGFactoryLoader.add_constructor(None, _dynamic_object_constructor)
