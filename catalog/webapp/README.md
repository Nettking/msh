# MSH Streamlit Web App (Legacy/Transitional)

This Streamlit app is now **legacy**.

- Primary web interface has moved to Flask (`catalog/flask_app/`).
- This Streamlit implementation remains temporarily for transition only.
- No further architectural expansion should target this module.

If you need the new default workflow, use:

```bash
python -m catalog.flask_app.app
```

or:

```bash
docker compose up --build webapp
```
