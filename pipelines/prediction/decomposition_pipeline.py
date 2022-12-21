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

client = kfp.Client(host='IP address')




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



@dsl.pipeline(
    name="croffle-decomposition",
    description = "croffle decompositio pipeline"
)
def decomposition_pipeline(cpu_request :str="4000m", 
                        cpu_limit : str="8000m",
                        memory_request : str="4000Mi",
                        memory_limit : str="16000Mi",
                        host_thr : int=60
                        ):
    # base image
    dsl.get_pipeline_conf().set_image_pull_secrets([kubernetes.client.V1LocalObjectReference(name="okestroaiops")])
    vop = dsl.PipelineVolume(pvc='croffle-pvc')    
    mount_path = '/symphony/'
 

    decomposition_vm_disk_write = decomposition_component('vm', 'diskio-write', host_thr).set_cpu_limit(cpu_limit)\
                                            .set_memory_limit(memory_limit)\
                                            .set_cpu_request(cpu_request)\
                                            .set_memory_request(memory_request)\
                                            .add_pvolumes({mount_path: vop})
                                            
    decomposition_vm_disk_read = decomposition_component('vm', 'diskio-read', host_thr).set_cpu_limit(cpu_limit)\
                                            .set_memory_limit(memory_limit)\
                                            .set_cpu_request(cpu_request)\
                                            .set_memory_request(memory_request)\
                                            .add_pvolumes({mount_path: vop})\
                                            .after(decomposition_vm_disk_write)