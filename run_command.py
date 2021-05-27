from instance_session import InstanceSession
import instance_state
import sys

if __name__ == '__main__':
    instance_types = instance_state.types_map.keys()
    if (len(sys.argv) < 3 or (sys.argv[1] not in instance_types)):
        print(
            f'Usage: {sys.argv[0]} {"|".join(instance_types)} command')
        sys.exit(1)

    session = InstanceSession(instance_state.types_map[sys.argv[1]])
    channels = session.all_run(sys.argv[2])
    for channel in channels:
        _, stdout, stderr = channel
        exit_status = stdout.channel.recv_exit_status()
        print('stdout:\n'
              f'{stdout.read().decode("unicode_escape")}\n'
              'stderr:\n'
              f'{stderr.read().decode("unicode_escape")}\n'
              f'exit status: {exit_status}\n=================================')
