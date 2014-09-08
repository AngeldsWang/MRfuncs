var cur = db.digg_friends_mutual.find().sort({"value.friendsNum": -1})
activeUsersIDs = [];
while (cur.hasNext()) {
	var curr = cur.next();
	activeUsersIDs.push(curr._id);
}

var mapFunction = function() {
	idx = activeUsersIDs.indexOf(this.voter_id)
	if (idx > -1) {
		var isodate = new Date(this.vote_date * 1000);
		var year = isodate.getFullYear();
		var month = isodate.getMonth() + 1;
		var date = isodate.getDate();
		if (date < 10) {
			var datestring = year.toString().concat("-")
										.concat(month.toString())
										.concat("-").concat("0")
										.concat(date.toString());

		} else {
			var datestring = year.toString().concat("-")
										.concat(month.toString())
										.concat("-")
										.concat(date.toString());
		}
		
		var users_voted_stories_ids = {};
		users_voted_stories_ids[this.voter_id] = [this.story_id]
		var value = {users_voted_stories: users_voted_stories_ids};

		emit(datestring, value);
	}
};

var reduceFunction = function(key, values) {
	
	var curr = {};

	for (i in values) {
		for (userid in values[i].users_voted_stories) {
			if (curr[userid] != null) {
				curr[userid] = curr[userid].concat(values[i].users_voted_stories[userid]);
			} else {
				curr[userid] = values[i].users_voted_stories[userid];
			}
		}
	}	
	return {users_voted_stories: curr};
};


db.digg_votes.mapReduce(mapFunction, reduceFunction, {"out": "digg_votes_by_day_mutual",
													  "scope": {activeUsersIDs: activeUsersIDs}});


