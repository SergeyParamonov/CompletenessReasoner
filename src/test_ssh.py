import paramiko
import os
username = os.environ["SSH_USERNAME"]
password = os.environ["SSS_PASSWORD"]
server   = "pinac34.cs.kuleuven.be"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(server, username=username, password=password)
cmd_to_execute = "hostname"
ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd_to_execute)
for line in ssh_stdout:
    print(line)
