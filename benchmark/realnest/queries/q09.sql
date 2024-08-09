SELECT unnested_urls.url FROM
    (SELECT unnest(unnested_users.entities.url.urls) AS unnested_urls FROM (
    SELECT unnest(includes.users) AS unnested_users
    FROM twitter)
);