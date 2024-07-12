from dbtr.server.lib.scheduler.gcp import GCPScheduler
from dbtr.server.lib.scheduler.azure import AzureScheduler
from dbtr.server.lib.scheduler.local import LocalScheduler

schedulers = {
    "google": GCPScheduler,
    "azure": AzureScheduler,
    "local": LocalScheduler,
}
