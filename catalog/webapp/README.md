# MSH Streamlit Web App (Archived)

This Streamlit app is now **archived**.

- Primary web interface has moved to Flask (`catalog/flask_app/`).
- This Streamlit implementation remains only as historical reference.
- No runtime, Docker, or operator workflow should target this module.

If you need the new default workflow, use:

```bash
python -m catalog.flask_app.app
```

or:

```bash
docker compose up --build flask
```
