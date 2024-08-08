select cr.entityID,
       cr.entityType,
       cr.boost,
       cr.collections,
       cr.status,
       cr.createdAt,
       cr.createdBy,
       cr.updatedAt,
       cr.updatedBy,
       cr.requestID
from edex_curations.Curation cr;