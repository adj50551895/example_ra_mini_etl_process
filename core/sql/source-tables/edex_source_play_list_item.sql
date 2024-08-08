SELECT p.id,
       p.playlistID,
       p.type,
       p.itemID,
       p.status,
       p.createdAt,
       p.updatedAt,
       p.deletedAt,
       p.sortOrder 
FROM edex_members.PlaylistItem p;