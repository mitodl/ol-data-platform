# ol_llm sets OPIK_SENTRY_ENABLE=false on import. Imported ahead of every module
# that imports opik, so the Opik SDK does not replace init_sentry("ml")'s client.
import ol_llm  # noqa: F401
