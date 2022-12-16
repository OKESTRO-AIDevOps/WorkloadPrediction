import kfp
from kfp import dsl
from kfp import components
from kfp.components import func_to_container_op
from elasticsearch import Elasticsearch
import kubernetes.client
from kubernetes.client.models.v1_toleration import V1Toleration
from kubernetes.client.models.v1_node_selector import V1NodeSelector
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm
from kubernetes.client.models.v1_affinity import V1Affinity
from kubernetes.client.models.v1_node_affinity import V1NodeAffinity
from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement

client = kfp.Client(host='http://34.64.153.235:30020')




def decomposition(provider: str,
                metric: str,
                host_thr: int=60):
    import sys
    sys.path.append('/symphony/croffle/pipelines/prediction/')

    from decomposition_main import main

    main(provider=provider, metric=metric, host_thr=host_thr)
    
 
decomposition_component = components.create_component_from_func(
        func=decomposition,
        base_image='okestroaiops/prediction:latest'
    )
