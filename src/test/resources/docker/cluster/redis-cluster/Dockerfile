FROM rockylinux:9.1

RUN yum install -y python3-pip redis
RUN pip install supervisor

ADD conf/supervisord.conf /opt/redis/supervisord.conf
ADD entrypoint.sh /bin/entrypoint.sh
ADD conf/ /opt/redis/

CMD /bin/entrypoint.sh

STOPSIGNAL 9