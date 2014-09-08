var mapFunction = function() {
	if (this.friend_date != 0 && this.mutual == 1) {
		var userid = this.user_id;
		var value = {friendsNum: 1,
					 friendsDate: [this.friend_date],
					 friendsIDs: [this.friend_id]};

		emit(userid, value);
	}
	
};

var reduceFunction = function(key, values) {
	var num = 0;
	var ids = [];
	var dates = [];
	for (i in values) {
		num += values[i].friendsNum;
		ids = ids.concat(values[i].friendsIDs);
		dates = dates.concat(values[i].friendsDate)
	}
	return {friendsNum: num,
			friendsDate: dates,
			friendsIDs: ids};
};


db.digg_friends.mapReduce(mapFunction, reduceFunction, {"out": "digg_friends_mutual"});


