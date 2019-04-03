package main

import (
	mysqlpkg "database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// CampaignFields - Fields from MySQL campaigns table
type CampaignFields struct {
	ID      int64
	Regions mysqlpkg.NullString
}

// BannerFields - Fields from MySQL campaigns table
type BannerFields struct {
	ID         int64
	CampaignID mysqlpkg.NullInt64
	Width      mysqlpkg.NullInt64
	Height     mysqlpkg.NullInt64
}

// BannerVideoFields - Fields from MySQL campaigns table
type BannerVideoFields struct {
	ID         int64
	CampaignID mysqlpkg.NullInt64
	Width      mysqlpkg.NullInt64
	Height     mysqlpkg.NullInt64
}

// Campaigns - Array of records with structure CampaignFields
type Campaigns []CampaignFields

// Banners - Array of records with structure BannerFields
type Banners []BannerFields

// BannerVideos - Array of records with structure BannerVideoFields
type BannerVideos []BannerVideoFields

var dbCampaignRecords = Campaigns{}
var dbBannerRecords = Banners{}
var dbBannerVideoRecords = BannerVideos{}

func readMySQLTables(mysqlHost string, mysqlDbname string, mysqlUser string, mysqlPassword string) bool {
	//	log1 := logger.GetLogger("readMySQLTables")
	var retError = false
	var dsn = mysqlUser + ":" + mysqlPassword + "@tcp(" + mysqlHost + ")/" + mysqlDbname
	db, err := mysqlpkg.Open("mysql", dsn)
	if err != nil {
		fmt.Println(err.Error())
		return true
	}
	defer db.Close()

	// RTB4FREE SQL table formats
	//recs := []CampaignFields{}

	iface, _ := executeMySQLSelect(db, "SELECT id,regions FROM campaigns", "campaign") // rtb4free table
	if camprecs, ok := iface.([]CampaignFields); ok {
		dbCampaignRecords = camprecs
	} else {
		return true
	}

	iface, _ = executeMySQLSelect(db, "SELECT id,campaign_id, width,height FROM banners", "banner") // rtb4free table
	if bannerrecs, ok := iface.([]BannerFields); ok {
		dbBannerRecords = bannerrecs
	} else {
		return true
	}

	iface, _ = executeMySQLSelect(db, "SELECT id,campaign_id, vast_video_width,vast_video_height FROM banner_videos", "video") // rtb4free table
	if videorecs, ok := iface.([]BannerVideoFields); ok {
		dbBannerVideoRecords = videorecs
	} else {
		return true
	}
	return retError
}

func executeMySQLSelect(db *mysqlpkg.DB, selectStmt string, rtype string) (interface{}, error) {
	log1 := logger.GetLogger("executeMySQLSelect")
	var rvals interface{}
	rows, err := db.Query(selectStmt)
	if err != nil {
		log1.Error(err.Error())
		return rvals, errors.New("Query error -" + selectStmt)
	}
	defer rows.Close()
	//if val, ok := rectype.(*[]CampaignFields); ok {
	switch rtype {
	case "campaign":
		recs := []CampaignFields{}
		count := 0
		for rows.Next() {
			rec := CampaignFields{}
			err = rows.Scan(&rec.ID, &rec.Regions)
			if err != nil {
				log1.Error(err.Error())
				return rvals, errors.New("Row error on select -" + selectStmt)
			}
			recs = append(recs, rec)
			count++
		}
		rvals = recs
		if err = rows.Err(); err != nil {
			log1.Error(err.Error())
			return rvals, errors.New("Rows error on select -" + selectStmt)
		}
		log1.Info(fmt.Sprintf("%d Campaign records read.", count))
	case "banner":
		recs := []BannerFields{}
		count := 0
		for rows.Next() {
			rec := BannerFields{}
			err = rows.Scan(&rec.ID, &rec.CampaignID, &rec.Width, &rec.Height)
			if err != nil {
				log1.Error(err.Error())
				return rvals, errors.New("Row error on select -" + selectStmt)
			}
			recs = append(recs, rec)
			count++
		}
		rvals = recs
		if err = rows.Err(); err != nil {
			log1.Error(err.Error())
			return rvals, errors.New("Rows error on select -" + selectStmt)
		}
		logger.Info(fmt.Sprintf("%d banner records read.", count))

	case "video":
		recs := []BannerVideoFields{}
		count := 0
		for rows.Next() {
			rec := BannerVideoFields{}
			err = rows.Scan(&rec.ID, &rec.CampaignID, &rec.Width, &rec.Height)
			if err != nil {
				log1.Error(err.Error())
				return rvals, errors.New("Row error on select -" + selectStmt)
			}
			recs = append(recs, rec)
			count++
		}
		rvals = recs
		if err = rows.Err(); err != nil {
			log1.Error(err.Error())
			return rvals, errors.New("Rows error on select -" + selectStmt)
		}
		log1.Info(fmt.Sprintf("%d video records read.", count))
	default:
		log1.Error("executeMySQLSelect can't find select type - ", rtype)
	}
	return rvals, nil
}

func (campaigns Campaigns) findID(ID int64) CampaignFields {
	log1 := logger.GetLogger("Campaigns findID")
	log1.Debug(fmt.Sprintf("Look in campaign records for id %d.", ID))
	for _, v := range campaigns {
		if v.ID == ID {
			return v
		}
	}
	return CampaignFields{}
}

func (banners Banners) findID(campID int64, ID int64) (BannerFields, bool) {
	log1 := logger.GetLogger("Banners findID")
	log1.Debug(fmt.Sprintf("Look in banner records for id %d.", ID))
	for _, v := range banners {
		if v.ID == ID && v.CampaignID.Int64 == campID {
			return v, true
		}
	}
	return BannerFields{}, false
}

func (videos BannerVideos) findID(campID int64, ID int64) (BannerVideoFields, bool) {
	log1 := logger.GetLogger("BannerVideos findID")
	log1.Debug(fmt.Sprintf("Look in video records for id %d.", ID))
	for _, v := range videos {
		if v.ID == ID && v.CampaignID.Int64 == campID {
			return v, true
		}
	}
	return BannerVideoFields{}, false
}
