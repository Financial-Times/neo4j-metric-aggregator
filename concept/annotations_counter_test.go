// +build integration

package concept

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/Financial-Times/go-logger/v2"
)

type AnnotationsCounterTestSuite struct {
	suite.Suite
	driver *cmneo4j.Driver
}

func TestNewAnnotationsCounterConnectionError(t *testing.T) {
	log := logger.NewUPPLogger("test-neo4j-metric-aggregator", "warning")
	driver, err := cmneo4j.NewDefaultDriver("bolt://localhost:80", log)
	require.NoError(t, err)

	ac := NewAnnotationsCounter(driver)

	_, err = ac.Count([]string{uuid.New().String()})
	assert.Error(t, err)
}

func TestAnnotationsCounterTestSuite(t *testing.T) {
	suite.Run(t, new(AnnotationsCounterTestSuite))
}

func (suite *AnnotationsCounterTestSuite) SetupTest() {
	log := logger.NewUPPInfoLogger("test-neo4j-metric-aggregator")
	neoTestURL := getNeoTestURL(suite.T())

	d, err := cmneo4j.NewDefaultDriver(neoTestURL, log)
	require.NoError(suite.T(), err)
	suite.driver = d
}

func (suite *AnnotationsCounterTestSuite) TearDownTest() {
	suite.cleanDB()
}

func (suite *AnnotationsCounterTestSuite) TestCountSingleValue() {
	conceptUUID := uuid.New().String()
	expectedAnnotationsCount := 25
	expectedRecentAnnotationsCount := 22
	suite.writeTestConceptWithAnnotations(conceptUUID, 3, expectedAnnotationsCount, expectedRecentAnnotationsCount)

	ac := NewAnnotationsCounter(suite.driver)
	counts, err := ac.Count([]string{conceptUUID})

	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), counts, 1)

	assert.Equal(suite.T(), int64(expectedRecentAnnotationsCount), counts[conceptUUID].PrevWeekAnnotationsCount)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount), counts[conceptUUID].AnnotationsCount)
}

func (suite *AnnotationsCounterTestSuite) TestCountMultiValue() {

	conceptUUID1 := uuid.New().String()
	expectedAnnotationsCount1 := 25
	expectedPrevAnnotationsCount1 := 9
	suite.writeTestConceptWithAnnotations(conceptUUID1, 3, expectedAnnotationsCount1, expectedPrevAnnotationsCount1)

	conceptUUID2 := uuid.New().String()
	expectedAnnotationsCount2 := 10
	expectedPrevAnnotationsCount2 := 4
	suite.writeTestConceptWithAnnotations(conceptUUID2, 1, expectedAnnotationsCount2, expectedPrevAnnotationsCount2)

	conceptUUID3 := uuid.New().String()
	expectedAnnotationsCount3 := 1234
	expectedPrevAnnotationsCount3 := 412
	suite.writeTestConceptWithAnnotations(conceptUUID3, 5, expectedAnnotationsCount3, expectedPrevAnnotationsCount3)

	conceptUUID4 := uuid.New().String()
	expectedAnnotationsCount4 := 0
	expectedPrevAnnotationsCount4 := 0
	suite.writeTestConceptWithAnnotations(conceptUUID4, 3, expectedAnnotationsCount4, expectedPrevAnnotationsCount4)

	uuids := []string{
		conceptUUID1,
		conceptUUID2,
		conceptUUID3,
		conceptUUID4,
	}

	ac := NewAnnotationsCounter(suite.driver)
	counts, err := ac.Count(uuids)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), counts, 4)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount1), counts[conceptUUID1].AnnotationsCount)
	assert.Equal(suite.T(), int64(expectedPrevAnnotationsCount1), counts[conceptUUID1].PrevWeekAnnotationsCount)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount2), counts[conceptUUID2].AnnotationsCount)
	assert.Equal(suite.T(), int64(expectedPrevAnnotationsCount2), counts[conceptUUID2].PrevWeekAnnotationsCount)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount3), counts[conceptUUID3].AnnotationsCount)
	assert.Equal(suite.T(), int64(expectedPrevAnnotationsCount3), counts[conceptUUID3].PrevWeekAnnotationsCount)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount4), counts[conceptUUID4].AnnotationsCount)
	assert.Equal(suite.T(), int64(expectedPrevAnnotationsCount4), counts[conceptUUID4].PrevWeekAnnotationsCount)
}

func (suite *AnnotationsCounterTestSuite) TestCountWithMissingConcepts() {
	conceptUUID1 := uuid.New().String()
	expectedAnnCount1 := 25
	expectedRecentAnnCount1 := 2
	suite.writeTestConceptWithAnnotations(conceptUUID1, 3, expectedAnnCount1, expectedRecentAnnCount1)

	conceptUUID2 := uuid.New().String()
	expectedAnnCount2 := 10
	expectedRecentAnnCount2 := 10
	suite.writeTestConceptWithAnnotations(conceptUUID2, 1, expectedAnnCount2, expectedRecentAnnCount2)

	conceptUUID3 := uuid.New().String()
	conceptUUID4 := uuid.New().String()

	uuids := []string{
		conceptUUID1,
		conceptUUID2,
		conceptUUID3,
		conceptUUID4,
	}

	ac := NewAnnotationsCounter(suite.driver)
	counts, err := ac.Count(uuids)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), counts, 2)
	assert.Equal(suite.T(), int64(expectedAnnCount1), counts[conceptUUID1].AnnotationsCount)
	assert.Equal(suite.T(), int64(expectedRecentAnnCount1), counts[conceptUUID1].PrevWeekAnnotationsCount)
	assert.Equal(suite.T(), int64(expectedAnnCount2), counts[conceptUUID2].AnnotationsCount)
	assert.Equal(suite.T(), int64(expectedRecentAnnCount2), counts[conceptUUID2].PrevWeekAnnotationsCount)
}

func (suite *AnnotationsCounterTestSuite) TestCountWithNoRecentAnnotations() {
	conceptUUID1 := uuid.New().String()
	expectedAnnCount1 := 125
	expectedRecentAnnCount1 := 0
	suite.writeTestConceptWithAnnotations(conceptUUID1, 3, expectedAnnCount1, expectedRecentAnnCount1)

	conceptUUID2 := uuid.New().String()
	expectedAnnCount2 := 10
	expectedRecentAnnCount2 := 0
	suite.writeTestConceptWithAnnotations(conceptUUID2, 1, expectedAnnCount2, expectedRecentAnnCount2)

	uuids := []string{
		conceptUUID1,
		conceptUUID2,
	}

	ac := NewAnnotationsCounter(suite.driver)
	counts, err := ac.Count(uuids)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), counts, 2)
	assert.Equal(suite.T(), int64(expectedAnnCount1), counts[conceptUUID1].AnnotationsCount)
	assert.Equal(suite.T(), int64(expectedRecentAnnCount1), counts[conceptUUID1].PrevWeekAnnotationsCount)
	assert.Equal(suite.T(), int64(expectedAnnCount2), counts[conceptUUID2].AnnotationsCount)
	assert.Equal(suite.T(), int64(expectedRecentAnnCount2), counts[conceptUUID2].PrevWeekAnnotationsCount)
}

func getNeoTestURL(t *testing.T) string {
	if testing.Short() {
		t.Skip("Skipping Neo4j integration tests.")
		return ""
	}

	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "bolt://localhost:7687"
	}

	return url
}

func (suite *AnnotationsCounterTestSuite) writeTestConceptWithAnnotations(conceptPrefUUID string, equivalentConcepts, totalAnnCount, recentAnnCount int) {
	// Create canonical concept node.
	canonicalQ := &cmneo4j.Query{
		Cypher: "CREATE (n:Concept{prefUUID: $prefUUID})",
		Params: map[string]interface{}{"prefUUID": conceptPrefUUID},
	}
	err := suite.driver.Write(canonicalQ)
	require.NoError(suite.T(), err)

	var sources []string
	for i := 0; i < equivalentConcepts; i++ {
		// create equivalent node
		equivalentConceptUUID := uuid.New().String()

		sourceQ := &cmneo4j.Query{
			Cypher: "MATCH (n:Concept{prefUUID: $prefUUID}) CREATE (n)<-[:EQUIVALENT_TO]-(x:Concept{uuid:$uuid})",
			Params: map[string]interface{}{"prefUUID": conceptPrefUUID, "uuid": equivalentConceptUUID},
		}
		err = suite.driver.Write(sourceQ)
		require.NoError(suite.T(), err)

		sources = append(sources, equivalentConceptUUID)
	}

	// Create annotations.
	writtenRecentAnn := 0
	for i := 0; i < totalAnnCount; i++ {
		isRecent := false
		if writtenRecentAnn < recentAnnCount {
			isRecent = true
		}

		// Get random source.
		sourceInd := rand.Int31n(int32(len(sources)))
		source := sources[sourceInd]

		pubDate := time.Now().Unix()
		if !isRecent {
			pubDate = pubDate - 7*24*3600 - 24*3600
		}

		contentQ := &cmneo4j.Query{
			Cypher: "MATCH (n:Concept{uuid: $uuid}) CREATE (n)<-[:REL]-(c:Content{publishedDateEpoch: $pubDate})",
			Params: map[string]interface{}{"uuid": source, "pubDate": pubDate},
		}
		err = suite.driver.Write(contentQ)
		require.NoError(suite.T(), err)

		writtenRecentAnn++
	}
}

func (suite *AnnotationsCounterTestSuite) cleanDB() {
	//delete content
	err := suite.driver.Write(&cmneo4j.Query{Cypher: "MATCH (n:Content) DETACH DELETE n"})
	require.NoError(suite.T(), err)
	//delete concepts
	err = suite.driver.Write(&cmneo4j.Query{Cypher: "MATCH (n:Concept) DETACH DELETE n"})
	require.NoError(suite.T(), err)
}
