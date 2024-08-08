SELECT f.id,
       f.entityID,
       f.entityType,
       f.status,
       f.createdAt,
       f.createdBy,
       f.updatedAt,
       f.updatedBy,
       f.requestID,
       f.siteID 
FROM edex_favorite.Favorite f;