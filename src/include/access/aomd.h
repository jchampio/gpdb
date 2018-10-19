/*-------------------------------------------------------------------------
 *
 * aomd.h
 *	  Declarations and functions for supporting aomd.c
 *
 * Portions Copyright (c) 2008, Greenplum Inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/access/aomd.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AOMD_H
#define AOMD_H

#include "htup_details.h"
#include "storage/fd.h"
#include "utils/rel.h"

extern int AOSegmentFilePathNameLen(Relation rel);

extern void FormatAOSegmentFileName(
						char *basepath,
						int segno,
						int col,
						int32 *fileSegNo,
						char *filepathname);

extern void MakeAOSegmentFileName(
					  Relation rel,
					  int segno,
					  int col,
					  int32 *fileSegNo,
					  char *filepathname);

extern File OpenAOSegmentFile(Relation rel,
				  char *filepathname,
				  int32 segmentFileNum,
				  int64	logicalEof);

extern void CloseAOSegmentFile(File fd);

extern void
TruncateAOSegmentFile(File fd,
					  Relation rel,
					  int32 segmentFileNum,
					  int64 offset);

extern void
mdunlink_ao(const char *path);

extern void
copy_append_only_data(RelFileNode src, RelFileNode dst, BackendId backendid, char relpersistence);

typedef enum
{
	AORELFILEOP_UPGRADE_FILES = 1,
	AORELFILEOP_UNLINK_FILES  = 2,
	AORELFILEOP_COPY_FILES = 3
} aoRelfileOperationType_t;

typedef struct aoRelFileOperationData {
	aoRelfileOperationType_t operation;
	union {
		struct {
			void *pageConverter;
			void *map;
		} upgradeFiles;
		struct {
			char *segPath;
			char *segpathSuffixPosition;
		} unlinkFiles;
		struct {
			char *srcPath;
			char *dstPath;
            RelFileNode dst;
			bool useWal;
		} copyFiles;
	} callbackData;
} aoRelFileOperationData_t;

/*
 * return value should be true if the this function correctly performed its
 *   underlying operation as expected on the segno and false otherwise.
 */
typedef bool (*aoRelFileFunction_t)(const int segno, const aoRelfileOperationType_t operation,
                            const aoRelFileOperationData_t *callbackArgs);

extern void
aoRelfileOperationExecute(const aoRelfileOperationType_t operation,
						  const aoRelFileFunction_t callback,
                          const aoRelFileOperationData_t *data);

#endif							/* AOMD_H */
