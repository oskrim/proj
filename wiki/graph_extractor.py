#!/usr/bin/env python3
"""
Graph extraction module using GLiNER and GLiREL for entity and relation extraction.
"""

from dataclasses import dataclass
from functools import cache
from typing import List, Tuple, Generator

import glirel  # noqa: F401 Import time side effect
import spacy
import logging
from gliner import GLiNER
from gliner.multitask import GLiNERRelationExtractor

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_DEVICE = "cpu"  # Can be overridden with CUDA if available
# Chunk sizes for different model types
GLIREL_CHUNK_SIZE = 1600  # Smaller chunks for glirel due to 512 token limit
# DEFAULT_CHUNK_SIZE = 10000  # Larger chunks for GLiNER-only models
DEFAULT_CHUNK_SIZE = 3200  # Larger chunks for GLiNER-only models
CHUNK_OVERLAP = 200  # Overlap between chunks to maintain context


@dataclass
class ExtractionResult:
    """Represents the result of entity and relation extraction from text.

    Contains lists of extracted entities and their relationships.
    """

    entities: List[Tuple[str, str]]  # (text, label)
    relations: List[Tuple[str, str, str]]  # (head_text, label, tail_text)


def chunk_text(text: str, chunk_size: int = DEFAULT_CHUNK_SIZE, chunk_overlap: int = CHUNK_OVERLAP) -> Generator[str, None, None]:
    """Split text into chunks while respecting sentence boundaries.

    Args:
        text: The text to chunk
        chunk_size: Target size for each chunk in characters
        chunk_overlap: Number of characters to overlap between chunks

    Yields:
        Text chunks that respect sentence boundaries
    """
    # Use spacy for sentence segmentation
    nlp = spacy.blank("en")
    nlp.add_pipe("sentencizer")
    doc = nlp(text)
    sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]

    if not sentences:
        return

    current_chunk = []
    current_size = 0

    for sentence in sentences:
        sentence_size = len(sentence) + 1  # +1 for space

        # If adding this sentence would exceed chunk_size, yield current chunk
        if current_size + sentence_size > chunk_size and current_chunk:
            yield " ".join(current_chunk)

            # Handle overlap by keeping some sentences from the end
            overlap_chunk = []
            overlap_size = 0

            # Add sentences from the end until we reach overlap size
            for sent in reversed(current_chunk):
                sent_size = len(sent) + 1
                if overlap_size + sent_size <= chunk_overlap:
                    overlap_chunk.insert(0, sent)
                    overlap_size += sent_size
                else:
                    break

            current_chunk = overlap_chunk + [sentence]
            current_size = overlap_size + sentence_size
        else:
            current_chunk.append(sentence)
            current_size += sentence_size

    # Don't forget the last chunk
    if current_chunk:
        yield " ".join(current_chunk)


def merge_extraction_results(results: List[ExtractionResult]) -> ExtractionResult:
    """Merge multiple extraction results, removing duplicates."""
    # Use sets to remove duplicates
    all_entities = set()
    all_relations = set()

    for result in results:
        all_entities.update(result.entities)
        all_relations.update(result.relations)

    return ExtractionResult(
        entities=list(all_entities),
        relations=list(all_relations)
    )


@cache
def nlp_model(threshold: float, entity_types: tuple[str], model_name: str = "urchade/gliner_mediumv2.1", device: str = DEFAULT_DEVICE):
    """Instantiate a spacy model with GLiNER and GLiREL components."""
    custom_spacy_config = {
        "gliner_model": model_name,
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


@cache
def multitask_model(model_name: str, device: str = DEFAULT_DEVICE):
    """Instantiate a GLiNER multitask model for direct extraction."""
    model = GLiNER.from_pretrained(model_name, map_location=device)
    return GLiNERRelationExtractor(model=model)


def extract_with_multitask(
    text: str,
    entity_types: List[str],
    relation_types: List[str],
    threshold: float,
    model_name: str,
    device: str = DEFAULT_DEVICE,
) -> ExtractionResult:
    """Extract entities and relations using GLiNER multitask model."""
    extractor = multitask_model(model_name, device)

    # Check if text needs chunking
    if len(text) <= DEFAULT_CHUNK_SIZE:
        # Process as single chunk
        predictions = extractor(text, entities=entity_types, relations=relation_types, threshold=threshold)
        logger.info(f"Predictions: {predictions}")

        # Handle different prediction formats
        entities = []
        relations = []
        
        # If predictions is a list (multitask model format)
        if isinstance(predictions, list):
            for pred in predictions:
                if isinstance(pred, dict):
                    # Each prediction has source, relation, target, score
                    if all(k in pred for k in ["source", "relation", "target", "score"]):
                        if pred["score"] >= threshold:
                            relations.append((pred["source"], pred["relation"], pred["target"]))
                        # Also extract entities from relations
                        entities.append((pred["source"], "ENTITY"))
                        entities.append((pred["target"], "ENTITY"))
        # If predictions is a dict (standard format)
        elif isinstance(predictions, dict):
            entities = [(ent["text"], ent["label"]) for ent in predictions.get("entities", [])]
            relations = [
                (rel["head"]["text"], rel["relation"], rel["tail"]["text"])
                for rel in predictions.get("relations", [])
                if rel["score"] >= threshold
            ]
        
        # Remove duplicate entities
        entities = list(set(entities))

        return ExtractionResult(entities=entities, relations=relations)

    # Process in chunks
    chunk_results = []
    for chunk in chunk_text(text, DEFAULT_CHUNK_SIZE, CHUNK_OVERLAP):
        predictions = extractor(chunk, entities=entity_types, relations=relation_types, threshold=threshold)
        logger.info(f"Predictions: {predictions}")

        # Handle different prediction formats
        entities = []
        relations = []
        
        # If predictions is a list (multitask model format)
        if isinstance(predictions, list):
            for pred in predictions:
                if isinstance(pred, dict):
                    # Each prediction has source, relation, target, score
                    if all(k in pred for k in ["source", "relation", "target", "score"]):
                        if pred["score"] >= threshold:
                            relations.append((pred["source"], pred["relation"], pred["target"]))
                        # Also extract entities from relations
                        entities.append((pred["source"], "ENTITY"))
                        entities.append((pred["target"], "ENTITY"))
        # If predictions is a dict (standard format)
        elif isinstance(predictions, dict):
            entities = [(ent["text"], ent["label"]) for ent in predictions.get("entities", [])]
            relations = [
                (rel["head"]["text"], rel["relation"], rel["tail"]["text"])
                for rel in predictions.get("relations", [])
                if rel["score"] >= threshold
            ]
        
        # Remove duplicate entities
        entities = list(set(entities))

        chunk_results.append(ExtractionResult(entities=entities, relations=relations))

    return merge_extraction_results(chunk_results)


def extract_rels(
    text: str,
    entity_types: List[str] = None,
    relation_types: List[str] = None,
    threshold: float = 0.75,
    model_name: str = "urchade/gliner_mediumv2.1",
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

    # Check if model name contains 'multi' for multitask model
    if 'multi' in model_name.lower():
        return extract_with_multitask(
            text=text,
            entity_types=entity_types,
            relation_types=relation_types,
            threshold=threshold,
            model_name=model_name,
            device=device
        )

    # Use spacy pipeline for other models - need chunking for glirel
    chunk_size = GLIREL_CHUNK_SIZE  # Use smaller chunks due to glirel's 512 token limit

    # Check if text needs chunking
    if len(text) <= chunk_size:
        # Process as single chunk
        nlp = nlp_model(threshold, tuple(entity_types), model_name, device)
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

    # Process in chunks for longer text
    nlp = nlp_model(threshold, tuple(entity_types), model_name, device)
    chunk_results = []

    for chunk in chunk_text(text, chunk_size, CHUNK_OVERLAP):
        docs = list(nlp.pipe([(chunk, {"glirel_labels": relation_types})], as_tuples=True))
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

        chunk_results.append(ExtractionResult(entities=ents, relations=rels))

    return merge_extraction_results(chunk_results)


def extract_graph_from_document(
    doc_id: str,
    title: str,
    content: str,
    entity_types: List[str] = None,
    relation_types: List[str] = None,
    threshold: float = 0.75,
    model_name: str = "urchade/gliner_mediumv2.1",
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
        model_name=model_name,
        device=device
    )
