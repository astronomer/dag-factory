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


class DAGFactoryLoader(yaml.FullLoader):
    pass


DAGFactoryLoader.add_constructor(
    "!kubernetes.client.models.V1ResourceRequirements", KubernetesConstructor.resource_requirements_constructor
)
DAGFactoryLoader.add_constructor("!kubernetes.client.models.V1Container", KubernetesConstructor.container_constructor)
DAGFactoryLoader.add_constructor("!kubernetes.client.models.V1PodSpec", KubernetesConstructor.pod_spec_constructor)
DAGFactoryLoader.add_constructor("!kubernetes.client.models.V1Pod", KubernetesConstructor.pod_constructor)
