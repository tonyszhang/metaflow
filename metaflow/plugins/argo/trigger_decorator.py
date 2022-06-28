from metaflow.decorators import FlowDecorator


class TriggerDecorator(FlowDecorator):
    name = "trigger"
    defaults = {
        "event": None,
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        self.triggers = self.attributes
