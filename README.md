# elasticsearch-reindex-problem


Zero downtime reindex problem.

Elasticsearch takes a snapshot of the source index and then slowly works its way through the snapshot to copy to the destination.

Any edits made against the SOURCE of the data are not replayed in the snapshot.

From failed google searching, all solutions point to using two aliases to handle this situation
1. read alias
2. write alias

However, this does not fix this problem. The solutions suggest to point the WRITE alias to the new index during reindex. But, let's assume that you want to DELETE a document. If it has not yet been reindex from _SOURCE_-->_DESTINATION_, the delete will fail with 404. This is fine, for now. But once the reindex process gets to document {{ID}} (that should have been deleted) it will then be cloned from _SOURCE_-->_DESTINATION_, where the previous *DELETE* that returned a 404 and was correct before is no longer correct.

The same is true for patch update operations. If the document you are trying to update *does not exist in the destination* at the time of the patch operation, it will fail. But once the reindex process gets around to it, it will then be updated with an incorrect version of the document.

