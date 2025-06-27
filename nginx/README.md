# Nginx configuration with cache

## Configuration

### Creating folders for cache

```shell
mkdir -p /var/cache/nginx/minio_cache
mkdir -p /var/cache/nginx/ceph_s3_cache

export MY_USER=$(grep -E '^user\s+' /etc/nginx/nginx.conf | awk '{print $2}' | sed 's/;$//')
if [ -z "$MY_USER" ]; then
    echo "Nginx user not found in /etc/nginx/nginx.conf"
    MY_USER="_nginx"
fi
export MY_GROUP=$(id -gn $MY_USER)
echo "nging user/group: ${MY_USER}:${MY_GROUP}"

chown -R ${MY_USER}:${MY_GROUP} /var/cache/nginx/minio_cache
chown -R ${MY_USER}:${MY_GROUP} /var/cache/nginx/ceph_s3_cache

ls -lah /var/cache/nginx/*

```

Note: User that nginx runs as can be taken from nginx.conf

### Enabling of cache


```
    proxy_cache_path /var/cache/nginx/s3_cache levels=1:2 keys_zone=S3-cache:512m inactive=240h max_size=50g;

```

`keys_zone=S3_cache` - name of cache (and folder created above)
`512m` - memory cache for each thread
`inactive=240h` - cache time to live
`max_size=50g` - max size that cache will take on disk

### Using cache in virtual server

Update `/etc/nginx/conf.d/minio.conf` - see `nginx/conf.d/minio.conf` in this repo.

Note: on some servers path can be `/etc/nginx/conf-available.d/` or so.



