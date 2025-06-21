# Nginx configuration with cache

## Configuration

### Creating folders for cache

```
mkdir -p /var/cache/nginx/s3_cache
chown -R nginx:nginx /var/cache/nginx/s3_cache
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



