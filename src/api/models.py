"""Request and response models for the pipeline API."""

from __future__ import annotations

from pydantic import BaseModel, Field

from src.descriptor.schema import EntityDescriptor
from src.descriptor.validator import DescriptorValidationError


class DescriptorValidateRequest(BaseModel):
    """Request body for Descriptor validation.

    Attributes:
        descriptor_uri: Local path or URI for the Descriptor TOML.
    """

    descriptor_uri: str = Field(min_length=1)


class DescriptorValidateResponse(BaseModel):
    """Response body for Descriptor validation.

    Attributes:
        descriptor_uri: Local path or URI that was validated.
        valid: Whether validation passed.
        descriptor: Parsed Descriptor when parsing succeeded.
        errors: Structured validation errors.
    """

    descriptor_uri: str
    valid: bool
    descriptor: EntityDescriptor | None = None
    errors: list[DescriptorValidationError] = Field(default_factory=list)


class RunRequest(BaseModel):
    """Request body for starting a pipeline run.

    Attributes:
        descriptors: Descriptor TOML URIs to process.
        output_root: Root path or URI for ecolib output data.
    """

    descriptors: list[str] = Field(min_length=1)
    output_root: str = Field(min_length=1)


class RunResponse(BaseModel):
    """Response body returned after a run is enqueued.

    Attributes:
        run_id: UUID string assigned to the run.
    """

    run_id: str
