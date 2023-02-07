/usr/local/bin/supervisord -c /opt/redis/supervisord.conf

touch /var/log/redis-cluster-stdout.log \
  /var/log/redis-50000-stdout.log \
  /var/log/redis-50001-stdout.log \
  /var/log/redis-50002-stdout.log \
  /var/log/redis-50003-stdout.log \
  /var/log/redis-50004-stdout.log \
  /var/log/redis-50005-stdout.log
  
touch /var/log/redis-cluster-stderr.log \
        /var/log/redis-50000-stderr.log \
        /var/log/redis-50001-stderr.log \
        /var/log/redis-50002-stderr.log \
        /var/log/redis-50003-stderr.log \
        /var/log/redis-50004-stderr.log \
        /var/log/redis-50005-stderr.log


tail --retry -fn30 /var/log/supervisord.log /var/log/redis*.log