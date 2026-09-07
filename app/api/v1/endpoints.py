import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.models import Endpoint
from app.schemas import EndpointCreate, EndpointResponse
from app.security import generate_secret_key

router = APIRouter(prefix="/api/v1/endpoints", tags=["endpoints"])


@router.post("", response_model=EndpointResponse, status_code=status.HTTP_201_CREATED)
async def create_endpoint(
    body: EndpointCreate, db: AsyncSession = Depends(get_db)
) -> Endpoint:
    endpoint = Endpoint(
        name=body.name,
        url=str(body.url),
        secret_key=generate_secret_key(),
        rate_limit_per_second=body.rate_limit_per_second,
        burst_capacity=body.burst_capacity,
    )
    db.add(endpoint)
    await db.commit()
    await db.refresh(endpoint)
    return endpoint


@router.get("", response_model=list[EndpointResponse])
async def list_endpoints(db: AsyncSession = Depends(get_db)) -> list[Endpoint]:
    result = await db.execute(select(Endpoint).order_by(Endpoint.created_at.desc()))
    return list(result.scalars().all())


@router.get("/{endpoint_id}", response_model=EndpointResponse)
async def get_endpoint(endpoint_id: uuid.UUID, db: AsyncSession = Depends(get_db)) -> Endpoint:
    endpoint = await db.get(Endpoint, endpoint_id)
    if endpoint is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Endpoint not found")
    return endpoint


@router.delete("/{endpoint_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_endpoint(endpoint_id: uuid.UUID, db: AsyncSession = Depends(get_db)) -> None:
    endpoint = await db.get(Endpoint, endpoint_id)
    if endpoint is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Endpoint not found")
    await db.delete(endpoint)
    await db.commit()
