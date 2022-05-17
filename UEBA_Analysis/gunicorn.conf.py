workers = 4 # 并行工作进程数
threads = 2 # 指定每个工作者的线程数
bind = '127.0.0.1:5055' # 端口 5055
daemon = 'false' # 设置守护进程,将进程交给supervisor管理
worker_class = 'gevent' # 工作模式协程
worker_connections = 2000 # 设置最大并发量
#pidfile = '/var/run/gunicorn.pid' # 设置进程文件目录

