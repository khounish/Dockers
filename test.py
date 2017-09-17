
from aarp.common.utils import createCluster

def createUserCluster():
    createCluster(name="userCluster_test", num_workers=3)

createUserCluster()