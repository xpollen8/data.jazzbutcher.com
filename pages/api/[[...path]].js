const urlencode = require('urlencode');
const Db = require('mywrap');

let db;

const queries = [
	{ noun: "press", query: "select * from press order by album, dtpublished desc" },
	{ noun: "interviews_by_person", query: "select * from press where type like '%interview%' and person='{{id}}' order by dtpublished desc" },
	{ noun: "posters", query: "select datetime from gig where extra like '%poster%' and isdeleted IS NULL order by datetime desc" },
	{ noun: "gigs", query: "select * from gig where isdeleted IS NULL" },
	{ noun: "gigs_and_year", query: "select *, year(datetime) as year from gig where isdeleted IS NULL order by datetime desc" },
	{ noun: "gigs_by_venue", query: "select *, year(datetime) as year from gig where venue like '%{{id}}%' and isdeleted IS NULL order by datetime desc" },
	{ noun: "gigs_and_year_by_id", query: "select *, year(datetime) as year from gig where find_in_set('{{id}}', extra) and isdeleted IS NULL order by datetime desc" },
	{ noun: "gigs_with_feedback", query: "select distinct(uri) from feedback where uri like 'gigs/%' and domain_id={{id}} order by uri desc" },
	{ noun: "gig_by_date", query: "select * from gig where datetime='{{id}}' and isdeleted IS NULL" },
	{ noun: "gigs_with_video", query: "select gig_id, datetime, venue, address, city, state, postalcode, country, extra, blurb, title from gig where find_in_set('video', extra) and isdeleted IS NULL order by datetime desc" },
	{ noun: "gigsong_with_media_by_date", query: "select * from gigsong where datetime='{{id}}' and mediaurl IS NOT NULL and isdeleted IS NULL order by ordinal, gigsong_id" },
	{ noun: "album_personnel", query: "select * from performance where lookup='{{id}}'" },
	{ noun: "album_press", query: "select * from press where album='{{id}}' order by dtpublished" },
	{ noun: "gigs_with_audio", query: "select gs.*, g.extra, g.venue, g.city, g.country from gigsong gs, gig g where gs.mediaurl like '%audio/%' and gs.datetime = g.datetime and g.isdeleted IS NULL group by g.datetime order by gs.datetime desc, gs.type desc, gs.setnum, gs.ordinal" },
	{ noun: "live_performances_by_song", query: 'select count(*) as cnt from gigsong where song="{{id}}"' },
	{ noun: "live_performances_with_media_by_song", query: 'select count(*) as cnt from gigsong where song="{{id}}" and length(mediaurl) > 0' },
];

const doQuery = async (res, obj, id) => {
	let q = JSON.parse(JSON.stringify(obj.query));
	if (obj.cache) {
		const results = obj.cache;
		console.log("CACHED", obj.noun);
		res.json({ 'numResults': results.length, 'rows': results });
		return;
	}
	if (q.indexOf('{{id}}') > 0) {
		if (id) {
			id = id.replace('+',' ');
			q = obj.query.replace('{{id}}', id);
		} else {
			//console.log("ID REQUIRED");
			res.json( { 'error': "ID REQUIRED" } );
			return;
		}
	}
	//console.log(q);
	await db.query(q, function (error, results, fields) {
		if (error) {
			res.json( { 'error': error } );
			return;
			//throw error;
		}
		if (!id) {	// only cache if no id
			obj.cache = results;
		}
		//res.json( results );
		res.json({ 'numResults': results.length, 'rows': results });
	});
}

const handler = async (req, res) => {
	try {
		if (!db) {
			db = await (new Db({
				host: process.env['JBC_MYSQL_HOST'],
				username: process.env['JBC_MYSQL_USERNAME'],
				user: process.env['JBC_MYSQL_USER'],
				database: process.env['JBC_MYSQL_DATABASE'],
				password: process.env['JBC_MYSQL_PASSWORD'],
				waitForConnection: process.env['JBC_MYSQL_WAITFORCONNECTION'],
				connectionLimit: process.env['JBC_MYSQL_CONNECTIONLIMIT'],
				queueLimit: process.env['JBC_MYSQL_QUEUELIMIT'],
			})).start();
		}
		//console.log("DB", db, process.env['JBC_MYSQL_HOST']);
		const { path = [] } = req.query;
		const [ noun, ...args ] = path;
		const obj = queries.find(q => q.noun === noun);
		if (obj) {
			await doQuery(res, obj, args[0]);
		} else {
			res.status(400).send({ error: `bad request` });
		}
	} catch (e) {
		console.log("ERROR", e);
		res.status(400).send(e);
	}
}

export default handler;
