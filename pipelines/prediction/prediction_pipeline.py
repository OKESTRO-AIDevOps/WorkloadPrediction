import kfp
from kfp import dsl
from kfp import components
from kfp.components import func_to_container_op
from elasticsearch import Elasticsearch
import kubernetes.client
client = kfp.Client(host='ip_address')




def prediction():
    import sys
    sys.path.append('path/your/prediction')

    from prediction_main import prediction_main
    prediction_main()
    
 
prediction_component = components.create_component_from_func(
        func=prediction,
        base_image='path/your/image'
    )

