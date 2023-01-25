/usr/local/bin/supervisord -c /opt/redis/supervisord.conf

touch /var/log/redis-50000-stdout.log \
  /var/log/redis-50001-stdout.log \
  /var/log/redis-50002-stdout.log \
  /var/log/redis-sentinel-51000-stdout.log \
  /var/log/redis-sentinel-51001-stdout.log \
  /var/log/redis-sentinel-51002-stdout.log
  
touch /var/log/redis-50000-stderr.log \
  /var/log/redis-50001-stderr.log \
  /var/log/redis-50002-stderr.log \
  /var/log/redis-sentinel-51000-stderr.log \
  /var/log/redis-sentinel-51001-stderr.log \
  /var/log/redis-sentinel-51002-stderr.log


tail --retry -fn30 /var/log/supervisord.log /var/log/redis*.log