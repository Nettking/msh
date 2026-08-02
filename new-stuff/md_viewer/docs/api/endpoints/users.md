# Users Endpoint

This file lives three folders deep, at
`docs/api/endpoints/users.md`, to demonstrate that the sidebar expands
every ancestor folder of the page you're viewing — **API Reference** and
**Endpoints** should both be open right now.

## List users

```
GET /v1/users
```

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique user identifier |
| `name` | string | Display name |
| `email` | string | Contact email |

## Get a single user

```
GET /v1/users/{id}
```

Returns a `404` if the user does not exist.
