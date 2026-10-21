# ruff: noqa: PLW0127, PLC0415
"""MIT Learn's point id and chunking code, copied for the parity tests.

Copied from mitodl/mit-learn at 2662d01e4edc3dee6a3a11fca1e9f9e8ed90a506,
``vector_search/utils.py``, ``vector_search/encoders/base.py`` and
``vector_search/constants.py``. Only Django ``settings`` is replaced, by the values
ol-infrastructure deploys (docs/learn_embedding_config.md). The function bodies
are unchanged apart from formatting, so that a change to the platform code
that breaks parity fails a test instead of being compared with itself.
``content_file_chunks`` is the loop body of ``_generate_content_file_points``
without the encoders and the Qdrant reads.
"""

import uuid
from functools import cache
from types import SimpleNamespace

settings = SimpleNamespace(
    LITELLM_TOKEN_ENCODING_NAME="cl100k_base",
    CONTENT_FILE_EMBEDDING_CHUNK_SIZE_OVERRIDE=512,
    CONTENT_FILE_EMBEDDING_CHUNK_OVERLAP=51,
)

QDRANT_CONTENT_FILE_PARAM_MAP = {
    "key": "key",
    "course_number": "course_number",
    "platform": "platform.code",
    "offered_by": "offered_by.code",
    "file_extension": "file_extension",
    "content_feature_type": "content_feature_type",
    "run_readable_id": "run_readable_id",
    "resource_readable_id": "resource_readable_id",
    "run_title": "run_title",
    "edx_module_id": "edx_module_id",
    "content_type": "content_type",
    "description": "description",
    "title": "title",
    "url": "url",
    "file_type": "file_type",
    "summary": "summary",
    "flashcards": "flashcards",
    "checksum": "checksum",
}

MARKDOWN_HEADERS_TO_SPLIT_ON = [
    ("#", "Header 1"),
    ("##", "Header 2"),
    ("###", "Header 3"),
    ("####", "Header 4"),
]


def model_short_name(model_name):
    split_model_name = model_name.split("/")
    model_name = model_name
    if len(split_model_name) > 1:
        model_name = split_model_name[1]
    return model_name


def vector_point_id(readable_id):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, readable_id))


def vector_point_key(
    serialized_document, chunk_number=0, document_type="learning_resource"
):
    platform = (serialized_document.get("platform") or {}).get("code", "")
    if document_type == "learning_resource":
        readable_id = serialized_document.get("readable_id") or serialized_document.get(
            "resource_readable_id"
        )
        return f"{platform}.{readable_id}"
    elif document_type == "course_information":
        return (
            f"{platform}."
            f"{serialized_document['readable_id']}."
            f"course_information.{chunk_number}"
        )
    elif document_type == "content_file":
        return (
            f"{platform}."
            f"{serialized_document['resource_readable_id']}."
            f"{serialized_document.get('run_readable_id', '')}."
            f"{serialized_document['key']}."
            f"{chunk_number}"
        )
    else:
        msg = "Invalid document type for vector point key"
        raise ValueError(msg)


@cache
def _get_text_splitter(**kwargs):
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    if settings.LITELLM_TOKEN_ENCODING_NAME:
        kwargs["encoding_name"] = settings.LITELLM_TOKEN_ENCODING_NAME
        return RecursiveCharacterTextSplitter.from_tiktoken_encoder(**kwargs)
    return RecursiveCharacterTextSplitter(**kwargs)


def _chunk_documents(texts, metadatas):
    chunk_params = {
        "chunk_overlap": settings.CONTENT_FILE_EMBEDDING_CHUNK_OVERLAP,
    }

    if settings.CONTENT_FILE_EMBEDDING_CHUNK_SIZE_OVERRIDE:
        chunk_params["chunk_size"] = settings.CONTENT_FILE_EMBEDDING_CHUNK_SIZE_OVERRIDE

    recursive_splitter = _get_text_splitter(**chunk_params)
    return recursive_splitter.create_documents(texts=texts, metadatas=metadatas)


def _is_markdown_content(doc):
    return (
        doc.get("file_type") == "marketing_page" or doc.get("file_extension") == ".md"
    )


def _chunk_markdown_documents(text, metadata):
    from langchain_text_splitters import MarkdownHeaderTextSplitter

    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=MARKDOWN_HEADERS_TO_SPLIT_ON,
        strip_headers=False,
    )
    header_docs = header_splitter.split_text(text)

    chunk_params = {
        "chunk_overlap": settings.CONTENT_FILE_EMBEDDING_CHUNK_OVERLAP,
    }
    if settings.CONTENT_FILE_EMBEDDING_CHUNK_SIZE_OVERRIDE:
        chunk_params["chunk_size"] = settings.CONTENT_FILE_EMBEDDING_CHUNK_SIZE_OVERRIDE

    recursive_splitter = _get_text_splitter(**chunk_params)
    split_docs = recursive_splitter.split_documents(header_docs)

    header_prefix_map = {key: prefix for prefix, key in MARKDOWN_HEADERS_TO_SPLIT_ON}
    for doc in split_docs:
        header_parts = [
            doc.metadata[key]
            for key in header_prefix_map
            if key in doc.metadata
            and not doc.page_content.startswith(
                f"{header_prefix_map[key]} {doc.metadata[key]}"
            )
        ]
        if header_parts:
            prefix = " > ".join(header_parts)
            doc.page_content = f"{prefix}\n\n{doc.page_content}"

    for doc in split_docs:
        doc.metadata.update(metadata)

    return split_docs


def _content_file_embedding_context(document):
    return document.get("content", "")


def _with_run_readable_id_fallback(serialized_document):
    if serialized_document.get("run_readable_id") or not serialized_document.get(
        "resource_readable_id"
    ):
        return serialized_document
    return {
        **serialized_document,
        "run_readable_id": serialized_document["resource_readable_id"],
    }


def content_file_chunks(doc):
    """Return ``(point_id, payload)`` for every chunk Learn writes for ``doc``."""
    embedding_context = _content_file_embedding_context(doc)
    if not embedding_context:
        return []
    if _is_markdown_content(doc):
        split_docs = _chunk_markdown_documents(embedding_context, doc)
    else:
        split_docs = _chunk_documents([embedding_context], [doc])

    valid_chunks = [(i, d) for i, d in enumerate(split_docs) if d.page_content]
    resource_vector_point_id = vector_point_id(vector_point_key(doc))
    points = []
    for chunk_id, split_doc in valid_chunks:
        metadata = _with_run_readable_id_fallback(
            {
                "resource_point_id": str(resource_vector_point_id),
                "chunk_number": chunk_id,
                "chunk_content": split_doc.page_content,
                **{
                    key: split_doc.metadata[key]
                    for key in QDRANT_CONTENT_FILE_PARAM_MAP
                    if key in split_doc.metadata
                },
            }
        )
        point_id = vector_point_id(
            vector_point_key(doc, chunk_number=chunk_id, document_type="content_file")
        )
        points.append((point_id, metadata))
    return points
