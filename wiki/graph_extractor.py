#!/usr/bin/env python3
"""
Graph extraction module using GLiNER and GLiREL for entity and relation extraction.
"""

from dataclasses import dataclass
from functools import cache
from typing import List, Tuple

import glirel  # noqa: F401 Import time side effect
import spacy

# Default configuration
DEFAULT_DEVICE = "cpu"  # Can be overridden with CUDA if available


@dataclass
class ExtractionResult:
    """Represents the result of entity and relation extraction from text.

    Contains lists of extracted entities and their relationships.
    """

    entities: List[Tuple[str, str]]  # (text, label)
    relations: List[Tuple[str, str, str]]  # (head_text, label, tail_text)


@cache
def nlp_model(threshold: float, entity_types: tuple[str], device: str = DEFAULT_DEVICE):
    """Instantiate a spacy model with GLiNER and GLiREL components."""
    custom_spacy_config = {
        "gliner_model": "urchade/gliner_mediumv2.1",
        "chunk_size": 250,
        "labels": entity_types,
        "style": "ent",
        "threshold": threshold,
        "map_location": device,
    }

    # Only require GPU if CUDA device is specified
    if device.startswith("cuda"):
        spacy.require_gpu()  # type: ignore

    nlp = spacy.blank("en")
    nlp.add_pipe("gliner_spacy", config=custom_spacy_config)
    nlp.add_pipe("glirel", after="gliner_spacy")
    return nlp


def extract_rels(
    text: str,
    entity_types: List[str] = None,
    relation_types: List[str] = None,
    threshold: float = 0.75,
    device: str = DEFAULT_DEVICE,
) -> ExtractionResult:
    """Extract entities and relations from text using GLiNER and GLiREL."""

    # Default entity and relation types for general knowledge extraction
    if entity_types is None:
        entity_types = [
            "PERSON", "ORGANIZATION", "LOCATION", "DATE", "TIME",
            "MONEY", "PERCENT", "FACILITY", "EVENT", "PRODUCT",
            "LAW", "LANGUAGE", "NORP"
        ]

    if relation_types is None:
        relation_types = [
            "located_in", "part_of", "member_of", "founded_by", "born_in",
            "died_in", "worked_for", "studied_at", "created_by", "owned_by",
            "leads", "manages", "collaborates_with", "related_to", "caused_by"
        ]

    nlp = nlp_model(threshold, tuple(entity_types), device)
    docs = list(nlp.pipe([(text, {"glirel_labels": relation_types})], as_tuples=True))
    relations = docs[0][0]._.relations

    sorted_data_desc = sorted(relations, key=lambda x: x["score"], reverse=True)

    # Extract entities
    ents = [(ent.text, ent.label_) for ent in docs[0][0].ents]

    # Extract relations
    rels = [
        (" ".join(item["head_text"]), item["label"], " ".join(item["tail_text"]))
        for item in sorted_data_desc
        if item["score"] >= threshold
    ]

    return ExtractionResult(entities=ents, relations=rels)


def extract_graph_from_document(
    doc_id: str,
    title: str,
    content: str,
    entity_types: List[str] = None,
    relation_types: List[str] = None,
    threshold: float = 0.75,
    device: str = DEFAULT_DEVICE,
) -> ExtractionResult:
    """
    Extract graph primitives from a document.

    Args:
        doc_id: Unique document identifier
        title: Document title
        content: Document content
        entity_types: List of entity types to extract
        relation_types: List of relation types to extract
        threshold: Confidence threshold for extraction
        device: Device to run extraction on

    Returns:
        ExtractionResult containing entities and relations
    """
    # Combine title and content for extraction
    full_text = f"{title}. {content}" if title else content

    return extract_rels(
        text=full_text,
        entity_types=entity_types,
        relation_types=relation_types,
        threshold=threshold,
        device=device
    )
