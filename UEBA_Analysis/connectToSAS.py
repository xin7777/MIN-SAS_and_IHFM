# encoding:UTF-8

import subprocess


def isConnected():
    proc = subprocess.Popen('nfdc route', shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, )
    stdout_value, stderr_value = proc.communicate()
    if str(stdout_value).find('/min/B/SAS') != -1:
        return True
    else:
        return False


def createFace():
    proc = subprocess.Popen('nfdc face create remote tcp://10.0.0.18:13899', shell=True, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, )
    stdoutValue, stderrValue = proc.communicate()
    stdoutStr = str(stdoutValue)
    if stdoutStr.find("face-created") == -1 and stdoutStr.find("face-exists") == -1:
        return -1

    # face-created id=4376 local=... 取中间id
    idPos = stdoutStr.find('id=')
    localPos = stdoutStr.find('local=')
    idStr = stdoutStr[idPos + 3: localPos - 1]
    return int(idStr)


def addRoute(faceId):
    proc = subprocess.Popen('nfdc route add prefix /min/B/SAS nexthop %d'%faceId, shell=True, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, )
    stdoutValue, stderrValue = proc.communicate()
    stdoutStr = str(stdoutValue)
    if stdoutStr.find("route-add-accepted") == -1:
        return False

    return True


if __name__ == '__main__':
    if isConnected():
        print("Already Connected to SAS.")
        exit(0)

    print("Start Creating Face.")
    faceId = createFace()
    if faceId == -1:
        print("Failed to create face.")
        exit(1)
    print("Face Created.")

    print("Start Adding Prefix.")
    if not addRoute(faceId):
        print("Failed to add SAS prefix.")
        exit(1)
    print("Prefix Added.")


