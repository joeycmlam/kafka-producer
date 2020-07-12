from datetime import datetime
from kafka import KafkaProducer


def send_message(p, topic_name, msg_value):
    try:
        msg_value_byte = bytes(msg_value, encoding='utf-8')
        p.send(topic_name, value=msg_value_byte)
        p.flush()
    except Exception as ex:
        print('exception @send_message')
        print(str(ex))

def publish_message_from_console(p, topic_name):
        try:
            continue_input = True

            while continue_input:
                myMessage = input('input message: ')
                if (myMessage == 'exit'):
                    continue_input = False
                else:
                    send_message(p, topic_name, myMessage)
                    print('message send')
        except Exception as ex:
            print('Excetion in publish_message_from_console')
            print(str(ex))

def publish_message_for_performance_test(p, topic_name):
    try:

        msg_idx = 0
        msg_max = 10000

        print('{}: start'.format(datetime.now()))
        while msg_idx < msg_max:
            msg_value = "message - [" + str(msg_idx) + "]"
            send_message(p, topic_name, msg_value)
            msg_idx = msg_idx + 1

        print('{}: done'.format(datetime.now()))
    except Exception as ex:
        print('exception at publish_message_from_console')
        print(str(ex))

if __name__ == '__main__':

    print ('start')
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    print('send message')
    #publish_message_from_console(producer, 'sample')
    publish_message_for_performance_test(producer, 'sample')
    print ('done')

