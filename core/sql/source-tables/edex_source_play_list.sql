SELECT p.id,
       p.title,
       p.memberID,
       p.siteID,
       p.status,
       p.createdAt,
       p.updatedAt,
       p.deletedAt,
       p.vanityURL,
       p.sortOrderCounter,
       p.activeItemsCount
FROM edex_members.Playlist p;