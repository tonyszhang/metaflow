from collections import defaultdict
import json
from metaflow import current
from metaflow.metaflow_config import KUBERNETES_NAMESPACE


class ArgoEvents(object):
    def __init__(self, workflow_template_ref, flow, username, production_token):
        # This implementation of Argo Events support for Metaflow defines a single
        # argo event sensor for each deployed argo workflow template. To keep things
        # simple, we currently set the workflow template name as the sensor name.
        #
        # This implementation side-steps the more prominent/popular usage of event
        # sensors where the sensor is responsible for submitting the workflow object
        # directly. Instead we construct the equivalent behavior of `argo submit
        # --from` to reference an already submitted workflow template. This ensures that
        # Metaflow generated Kubernetes objects can be easily reasoned about.
        #
        # NOTE: If a user defined parameter has a default associated with it, then the
        #       configured event will trigger the workflow even if the payload for the
        #       event can't be mapped to the parameter. If no default is specified,
        #       then the workflow isn't triggered.

        self.name = workflow_template_ref
        self.flow = flow
        self.username = username
        self.production_token = production_token
        self.workflow_template_ref = workflow_template_ref

    def sensor(self):
        trigger = self.flow._flow_decorators.get("trigger")
        if trigger:
            # https://docs.google.com/document/d/1liTvpACWKioCSQTUv5iO3g2AKuLu4x3EYFwEl43WAZU
            # Triggers can be of the format -
            # 1. {"event": "event_name"}
            # 2. {"event": {"name" : "event_name", "parameters": {"foo": "bar"}}}
            if "event" in trigger.triggers:
                # {"event": "event_name"}
                if type(trigger.triggers["event"]) is str:
                    event_name = trigger.triggers["event"]
                    parameters = {}
                # TODO: Handle event sources as well for Argo
                # {"event": {"name" : "event_name", "parameters": {"foo": "bar"}}}
                else:
                    # TODO: Handle dict parsing.. check "name", "parameters" exist
                    event_name = trigger.triggers["event"]["name"]
                    parameters = trigger.triggers["event"].get("parameters")

            # Verify parameters
            if parameters:
                for parameter in parameters:
                    print(parameter)
                print(parameters)

            # We set the same annotations and labels as for Argo Workflow Templates at
            # the moment.
            annotations = {
                "metaflow/production_token": self.production_token,
                "metaflow/owner": self.username,
                "metaflow/user": "argo-workflows",
                "metaflow/flow_name": self.flow.name,
            }
            if current.get("project_name"):
                annotations.update(
                    {
                        "metaflow/project_name": current.project_name,
                        "metaflow/branch_name": current.branch_name,
                        "metaflow/project_flow_name": current.project_flow_name,
                    }
                )

            return (
                Sensor()
                .metadata(
                    ObjectMeta()
                    .name(self.name)
                    .namespace(KUBERNETES_NAMESPACE)
                    .label("app.kubernetes.io/name", "metaflow-trigger")
                    .label("app.kubernetes.io/part-of", "metaflow")
                    .annotations(annotations)
                )
                .spec(
                    SensorSpec()
                    # TODO: Make this configurable
                    .service_account_name("operate-workflow-sa")
                    .dependencies(
                        [
                            EventDependency(event_name)
                            .event_name(event_name)
                            .event_source_name("webhook")
                        ]
                    )
                    .trigger(
                        Trigger().template(
                            TriggerTemplate()
                            .name("%s-trigger" % self.name)
                            .argo_workflow_trigger(
                                ArgoWorkflowTrigger().trigger(
                                    self.workflow_template_ref, parameters
                                )
                                # .parameter(
                                #     TriggerParameter()
                                #     .src("example", "body")
                                #     .dest("spec.arguments.parameters.0.value")
                                # )
                            )
                            # TODO: Set conditions
                            .conditions(event_name)
                        )
                    )
                )
            )

            print(trigger.trigger_events)
        return None


# Helper classes to assist with JSON-foo. This can very well replaced with an explicit
# dependency on argo-events Python SDK if this method turns out to be painful.
# TODO: Autogenerate them, maybe?


class Sensor(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["apiVersion"] = "argoproj.io/v1alpha1"
        self.payload["kind"] = "Sensor"

    def metadata(self, object_meta):
        self.payload["metadata"] = object_meta.to_json()
        return self

    def spec(self, sensor_spec):
        self.payload["spec"] = sensor_spec.to_json()
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class SensorSpec(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.SensorSpec

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def dependencies(self, dependencies):
        if "dependencies" not in self.payload:
            self.payload["dependencies"] = []
        for dependency in dependencies:
            self.payload["dependencies"].append(dependency.to_json())
        return self

    def trigger(self, trigger):
        if "triggers" not in self.payload:
            self.payload["triggers"] = []
        self.payload["triggers"].append(trigger.to_json())
        return self

    def template(self, template):
        self.payload["template"] = template.to_json()
        return self

    def service_account_name(self, service_account_name):
        self.payload["template"] = {"serviceAccountName": service_account_name}
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class EventDependency(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.EventDependency

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def event_name(self, event_name):
        self.payload["eventName"] = event_name
        return self

    def event_source_name(self, event_source_name):
        self.payload["eventSourceName"] = event_source_name
        return self

    def filters(self, event_dependency_filter):
        self.payload["filters"] = event_dependency_filter
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Trigger(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.Trigger

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def template(self, trigger_template):
        self.payload["template"] = trigger_template.to_json()
        return self

    # def parameter(self, trigger_parameter):
    #     if "parameters" not in self.payload:
    #         self.payload["parameters"] = []
    #     self.payload["parameters"].append(trigger_parameter.to_json())
    #     return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class TriggerTemplate(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.TriggerTemplate

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def name(self, name):
        self.payload["name"] = name
        return self

    def conditions(self, conditions):
        # self.payload["conditions"] = conditions
        return self

    def argo_workflow_trigger(self, argo_workflow_trigger):
        self.payload["argoWorkflow"] = argo_workflow_trigger.to_json()
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class ArgoWorkflowTrigger(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.ArgoWorkflowTrigger

    # workflow template ref
    # parameters
    #

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["operation"] = "submit"
        self.payload["group"] = "argoproj.io"
        self.payload["version"] = "v1alpha1"
        self.payload["resource"] = "workflows"

    def trigger(self, workflow_template_ref, parameters):
        self.payload["source"] = {
            "resource": {
                "apiVersion": "argoproj.io/v1alpha1",
                "kind": "Workflow",
                "metadata": {
                    "generateName": "%s-" % workflow_template_ref,
                    "namespace": KUBERNETES_NAMESPACE,
                },
                "spec": {
                    "arguments": {
                        "parameters": [
                            {"name": "alpha", "value": "1"},
                            {"name": "beta", "value": "2"},
                        ]
                    },
                    "workflowTemplateRef": {
                        "name": workflow_template_ref,
                    },
                },
            }
        }
        print(parameters)
        return self

    def parameter(self, trigger_parameter):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        self.payload["parameters"].append(trigger_parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class TriggerParameter(object):
    # https://github.com/argoproj/argo-events/blob/master/api/sensor.md#argoproj.io/v1alpha1.TriggerParameter

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def src(self, dependency_name, data_key):
        self.payload["src"] = {
            "dependencyName": dependency_name,
            "dataKey": "body",
            "value": "foo",
        }
        return self

    def dest(self, dest):
        self.payload["dest"] = dest
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class ObjectMeta(object):
    # https://argoproj.github.io/argo-workflows/fields/#objectmeta

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def annotation(self, key, value):
        self.payload["annotations"][key] = str(value)
        return self

    def annotations(self, annotations):
        if "annotations" not in self.payload:
            self.payload["annotations"] = {}
        self.payload["annotations"].update(annotations)
        return self

    def label(self, key, value):
        self.payload["labels"][key] = str(value)
        return self

    def labels(self, labels):
        if "labels" not in self.payload:
            self.payload["labels"] = {}
        self.payload["labels"].update(labels)
        return self

    def name(self, name):
        self.payload["name"] = name
        return self

    def namespace(self, namespace):
        self.payload["namespace"] = namespace
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)
