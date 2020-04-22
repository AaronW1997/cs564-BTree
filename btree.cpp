/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	this->bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;

	//get the index name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	Page* mdpage;
	IndexMetaInfo* md;
	BlobFile* bfile;

	if(badgerdb::BlobFile::exists(outIndexName) == true)
	{
		bfile = new BlobFile(outIndexName, false);
		file = (File*)bfile;

		headerPageNum = file->getFirstPageNo();
		bufMgr->readPage(file, headerPageNum, mdpage);

		md = (IndexMetaInfo*)mdpage;
		rootPageNum = md->rootPageNo;
		bufMgr->unPinPage(file, headerPageNum, false);
	}
	else
	{
		bfile = new BlobFile(outIndexName, true);
		file = (File*)bfile;

		Page *headpage;
		Page *rootpage;

		bufMgr->allocPage(file, headerPageNum, headpage);
    	bufMgr->allocPage(file, rootPageNum, rootpage);

		md = (IndexMetaInfo*)headpage;
		md->attrByteOffset = attrByteOffset;
		md->attrType = attrType;
		md->rootPageNo = rootPageNum;

		bufMgr->unPinPage(file, headerPageNum, true);
    	bufMgr->unPinPage(file, rootPageNum, true);
	}

	FileScan fscan(relationName, bufMgr);
	RecordId rid;
	try
	{
		fscan.scanNext(rid);
		std::string record = fscan.getRecord();
		insertEntry(record.c_str() + attrByteOffset, rid);
	}
	catch(EndOfFileException e)
	{
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	scanExecuting = false;
	bufMgr->flushFile(BTreeIndex::file);
	delete file;
	file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
