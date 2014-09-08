var mapFunction = function() {
	var authors = this.Authors;
	var onepaperauthorids = [];
	// get all authors' id of a paper
	for(one in authors) {
		onepaperauthorids.push(one);
	}

	// get all refs' ids
	var refs = this.refIDs;
	var refIDNums = {};
	for (var ref = 0; ref < refs.length; ref++) {
		refIDNums[refs[ref]] = 1;
	}

	// keep the paper's author list
	var coauthorIDs = onepaperauthorids;

	for(id in authors) {
		onepaperauthorids = coauthorIDs;
		var index = onepaperauthorids.indexOf(id);

		if (index > -1) {
			// count coauthor num
			onepaperauthorids.splice(index, 1);
			var coauthorIDNums = {};
			for(var idx = 0; idx < onepaperauthorids.length; idx++) {
				coauthorIDNums[onepaperauthorids[idx]] = 1;
			}

			var paperConfIDNum = {};
			paperConfIDNum[this.confName] = 1;

			var paperYearNum = {};
			paperYearNum[this.Year] = 1;

			var value = {authorID: id,
						 authorName: authors[id],
						 paperNum: 1,
						 paperTiltes: [this.paperTitle],
						 paperAbstracts: [this.Abstract],
						 paperConfs: paperConfIDNum,
						 paperYears: paperYearNum,
						 paperRefs: refIDNums,
						 coauthors: coauthorIDNums};

			emit(id, value);
		}
	}
};

var reduceFunction = function(key, values) {
	var currPaperNum = 0;
	var titleCurr = [];
	var abstractCurr = [];
	var confNameCurr = {};
	var paperYearCurr = {};
	var paperRefCurr = {};
	var coauthorCurr = {};

	for (i in values) {
		currPaperNum += values[i].paperNum;
		titleCurr = titleCurr.concat(values[i].paperTiltes);
		abstractCurr = abstractCurr.concat(values[i].paperAbstracts);
		// add up the conf nums
		for (conf in values[i].paperConfs) {
			if (confNameCurr[conf] != null) {
				confNameCurr[conf] += values[i].paperConfs[conf];
			} else {
				confNameCurr[conf] = values[i].paperConfs[conf];
			}
		}
		

		// add up the year nums
		for (year in values[i].paperYears) {
			if (paperYearCurr[year] != null) {
				paperYearCurr[year] += values[i].paperYears[year];
			} else {
				paperYearCurr[year] = values[i].paperYears[year];
			}
		}
		
		
		// add up the refs nums
		for (eachref in values[i].paperRefs) {
			if (paperRefCurr[eachref] != null) {
				paperRefCurr[eachref] += values[i].paperRefs[eachref];
			} else {
				paperRefCurr[eachref] = values[i].paperRefs[eachref];
			}
		}

		// add up the coauthor nums
		for (author in values[i].coauthors) {
			if (coauthorCurr[author] != null) {
				coauthorCurr[author] += values[i].coauthors[author];
			} else {
				coauthorCurr[author] = values[i].coauthors[author];
			}
		}
	}

	
	return {authorID: values[0].authorID,
			authorName: values[0].authorName,
			paperNum: currPaperNum,
			paperTiltes: titleCurr,
			paperAbstracts: abstractCurr,
			paperConfs: confNameCurr,
			paperYears: paperYearCurr,
			paperRefs: paperRefCurr,
			coauthors: coauthorCurr};
};


db.first_10000.mapReduce(mapFunction, reduceFunction, {"out": "AuthorsInfo"});


