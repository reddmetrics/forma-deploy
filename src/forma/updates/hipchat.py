import os, subprocess

hipchat_message = 'echo "I haz updated: \n%s" | ~/hipchat-cli/hipchat_room_message -t %s -r 42482 -f "UpdaterBot"'

def send_message(msg, cmd=hipchat_message):
    if os.getenv("hipchat"):
        try:
            subprocess.check_call(cmd % (msg, os.getenv("hipchat")), shell=True)
        except:
            pass

