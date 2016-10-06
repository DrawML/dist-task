from dist_system.master import main

if __name__ == '__main__':
    main(client_router_addr='tcp://*:16000',
         slave_router_addr='tcp://*:17000')
