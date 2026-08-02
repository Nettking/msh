# Welcome to Stacks

This is a small **markdown documentation viewer** built with Flask. It walks
the `docs/` folder (including all subfolders) and turns whatever `.md` files
it finds into a browsable site, with the folder structure mirrored in the
navigation on the left.

## How it works

1. Drop `.md` files anywhere inside `docs/` — flat or in nested subfolders.
2. Start the server with `python app.py`.
3. The sidebar rebuilds itself from the folder tree on every request, so new
   files show up automatically without restarting anything.

## Try it out

- Open **Guide → Getting Started** in the sidebar.
- Open **API Reference → Endpoints → Users** to see a deeper, nested path.

> The folder containing whichever page you're reading stays expanded in the
> sidebar, so you always know where you are in the tree.

```python
# Example: this is what a fenced code block looks like
def hello():
    return "hello from the docs"
```

| Feature | Status |
|---|---|
| Nested folders | ✅ |
| Syntax highlighting | ✅ |
| Tables | ✅ |
| Auto-refresh on new files | ✅ |
