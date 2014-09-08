var cur = db.activeAuthorsallyears.find();
var activeAuthorids = [];
while(cur.hasNext()) {
	var curr = cur.next();
	var id = curr["authorid"];
	activeAuthorids.push(id);
}

// var Years = [];
// var START_YEAR = 1993;
// var END_YEAR = 2012;
// for (var i = START_YEAR; i <= END_YEAR; i++) {
// 	Years.push(i);
// }

// build the key-value pattern as
// key: authorid
// value:
//	{
// 		"authorid":id
//		"authorName": name
//		"yearslices": {
// 			year1: {
//				"paperNum": num
//				"paperTitiles": [String Array]
//				"paperAbstracts": [String Array]
//				"paperConfs" {
//					confid: num_in_this_conf
//				 	 ... : ...		
//				}
//				"paperRefs" {
//					paperid: num_ref_this_paper
//					  ... : ...		
//				}
//				"coauthors" {
//					authorid: num_work_with_this_author
//					  ... : ...		
//				}
//			}
//			year2: {
//				... : ...
//			}
//			... : ...
//		}	
//	}

var mapFunction = function() {
	var authors = this.Authors;
	//var authorsPapersNum = this.authorsPapersNum;
	var onepaperauthorids = [];
	//var thispaperAuthorsPapersNum = [];
	// get all authors' id of a paper
	for (one in authors) {
		onepaperauthorids.push(one);
	}

	// // get this paper's authors' paperNum
	// for (oneauthor in authorsPapersNum) {
	// 	thispaperAuthorsPapersNum.push(oneauthor);
	// }

	// thispaperAuthorsPapersNum.sort();
	// maxpaperNum = thispaperAuthorsPapersNum[thispaperAuthorsPapersNum.length - 1];
	// minpaperNum = thispaperAuthorsPapersNum[0];

	// get all refs' ids
	var refs = this.refIDs;
	var refIDNums = {};
	for (var ref = 0; ref < refs.length; ref++) {
		refIDNums[refs[ref]] = 1;
	}

	var paperConfIDNum = {};
	paperConfIDNum[this.confName] = 1;

	

	for(id in authors) {
		// copy the paper's author list (!!!!!deep copy)
		var coauthorIDs = [];
		for(var i = 0; i < onepaperauthorids.length; i++) {
			coauthorIDs[i] = onepaperauthorids[i];
		}
		var index = coauthorIDs.indexOf(id);
		var idx = activeAuthorids.indexOf(id);

		if (index > -1 && idx > -1) {
			// count coauthor num
			coauthorIDs.splice(index, 1);
			var coauthorIDNums = {};
			for(var idx = 0; idx < coauthorIDs.length; idx++) {
				coauthorIDNums[coauthorIDs[idx]] = 1;
			}

			var thisyear = {};
			thisyear[this.Year] = {
				paperNum: 1,
				paperTitles: [this.paperTitle],
				paperAbstracts: [this.Abstract],
				paperConfs: paperConfIDNum,
				paperRefs: refIDNums,
				coauthors: coauthorIDNums
			}

			var value = {authorID: id,
						 authorName: authors[id],
						 yearslices: thisyear};

			emit(id, value);
		}		
	}
};

var reduceFunction = function(key, values) {
	var currPaperNum = 0;
	var titleCurr = [];
	var abstractCurr = [];
	var confNameCurr = {};
	var paperRefCurr = {};
	var coauthorCurr = {};
	var yearsliceCurr = {};

	for (i in values) {
		for (year in values[i].yearslices) {
			// if this year has been counted
			if (yearsliceCurr[year] != null) {
				// load this year current data
				currPaperNum = yearsliceCurr[year].paperNum;
				titleCurr = yearsliceCurr[year].paperTitles;
				abstractCurr = yearsliceCurr[year].paperAbstracts;
				confNameCurr = yearsliceCurr[year].paperConfs;
				paperRefCurr = yearsliceCurr[year].paperRefs;
				coauthorCurr = yearsliceCurr[year].coauthors;

				// accumulate the six fields
				currPaperNum += values[i].yearslices[year].paperNum;
				titleCurr = titleCurr.concat(values[i].yearslices[year].paperTitles);
				abstractCurr = abstractCurr.concat(values[i].yearslices[year].paperAbstracts);
				// add up the conf nums
				for (conf in values[i].yearslices[year].paperConfs) {
					if (confNameCurr[conf] != null) {
						confNameCurr[conf] += values[i].yearslices[year].paperConfs[conf];
					} else {
						confNameCurr[conf] = values[i].yearslices[year].paperConfs[conf];
					}
				}
				
				// add up the refs nums
				for (eachref in values[i].yearslices[year].paperRefs) {
					if (paperRefCurr[eachref] != null) {
						paperRefCurr[eachref] += values[i].yearslices[year].paperRefs[eachref];
					} else {
						paperRefCurr[eachref] = values[i].yearslices[year].paperRefs[eachref];
					}
				}

				// add up the coauthor nums
				for (author in values[i].yearslices[year].coauthors) {
					if (coauthorCurr[author] != null) {
						coauthorCurr[author] += values[i].yearslices[year].coauthors[author];
					} else {
						coauthorCurr[author] = values[i].yearslices[year].coauthors[author];
					}
				}

				// update the accumulative value
				yearsliceCurr[year] = {
					paperNum: currPaperNum,
					paperTitles: titleCurr,
					paperAbstracts: abstractCurr,
					paperConfs: confNameCurr,
					paperRefs: paperRefCurr,
					coauthors: coauthorCurr
				}
			} else {	// if this year has not been counted
				yearsliceCurr[year] = values[i].yearslices[year];

				// init the current six fields
				currPaperNum = values[i].yearslices[year].paperNum;
				titleCurr = titleCurr.concat(values[i].yearslices[year].paperTitles);
				abstractCurr = abstractCurr.concat(values[i].yearslices[year].paperAbstracts);
				confNameCurr = values[i].yearslices[year].paperConfs;
				paperRefCurr = values[i].yearslices[year].paperRefs;
				coauthorCurr = values[i].yearslices[year].coauthors;
			}
		}
	}

	
	return {authorID: values[0].authorID,
			authorName: values[0].authorName,
			yearslices: yearsliceCurr};
};


db.DBLP_citation_Sep_2013.mapReduce(mapFunction, 
									reduceFunction, 
									{"out": "activeAuthors20yearSliceInfo",
									 "scope": {activeAuthorids: activeAuthorids}});


