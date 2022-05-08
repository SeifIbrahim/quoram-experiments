from instance_session import InstanceSession, INSTANCE_TYPES
import sys

if __name__ == '__main__':
    instance_names = INSTANCE_TYPES.keys()
    if (len(sys.argv) < 3 or sys.argv[1] not in ['start', 'stop']
            or sys.argv[2] not in instance_names):
        print(f'Usage: {sys.argv[0]} start|stop {"|".join(instance_names)}')
        sys.exit(1)

    session = InstanceSession(INSTANCE_TYPES[sys.argv[2]])
    if sys.argv[1] == 'start':
        session.start_instances()
    else:
        session.stop_instances()
