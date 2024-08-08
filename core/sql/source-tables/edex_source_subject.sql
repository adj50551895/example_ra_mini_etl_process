select sb.id,
       sb.createdAt,
       sb.updatedAt,
       sb.parentID,
       sb.i18nLabel,
       sb.urlLabel,
       sb.imageURL,
       sb.status
from edex_metadata.Subject sb;
