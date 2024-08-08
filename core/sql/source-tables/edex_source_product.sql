SELECT p.id,
       p.createdAt,
       p.updatedAt,
       p.siteID,
       p.title,
       p.urlLabel,
       p.description,
       p.vanityURL,
       p.links,
       p.iconProps,
       p.classifications,
       p.creativeCloud,
       p.selectable,
       p.featured,
       p.sortOrder,
       p.status,
       p.helpfulInfo
FROM edex_metadata.Product p;

