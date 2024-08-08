SELECT r.id,
       r.rating,
       r.siteID,
       r.entityID,
       r.entityType,
       r.createdAt,
       r.createdBy,
       r.updatedAt,
       r.updatedBy,
       r.requestID
FROM edex_ratings.Rating r;