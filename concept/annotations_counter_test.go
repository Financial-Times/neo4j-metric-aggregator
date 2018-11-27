// +build integration

package concept

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const recentAnnotationsCountAge = 7 * 24 * 3600

type AnnotationsCounterTestSuite struct {
	suite.Suite
	driverPool bolt.DriverPool
}

func TestNewAnnotationsCounterConnectionError(t *testing.T) {
	dp, err := bolt.NewDriverPool("bolt://localhost:80", 10)
	require.NoError(t, err)
	ac := NewAnnotationsCounter(dp, recentAnnotationsCountAge)

	_, err = ac.Count([]string{uuid.New().String()})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error in creating a connection to Neo4j:")
}

func TestAnnotationsCounterTestSuite(t *testing.T) {
	suite.Run(t, new(AnnotationsCounterTestSuite))
}

func (suite *AnnotationsCounterTestSuite) SetupTest() {
	neoTestURL := getNeoTestURL(suite.T())
	dp, err := bolt.NewDriverPool(neoTestURL, 10)
	require.NoError(suite.T(), err)
	suite.driverPool = dp
}

func (suite *AnnotationsCounterTestSuite) TearDownTest() {
	suite.cleanDB()
}

func (suite *AnnotationsCounterTestSuite) TestCountSingleValue() {
	conceptUUID := uuid.New().String()
	expectedAnnotationsCount := 25
	suite.writeTestConceptWithAnnotations(conceptUUID, 3, expectedAnnotationsCount)

	ac := NewAnnotationsCounter(suite.driverPool, recentAnnotationsCountAge)
	counts, err := ac.Count([]string{conceptUUID})

	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), counts, 1)

	assert.Equal(suite.T(), int64(9), counts[conceptUUID].Recent)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount), counts[conceptUUID].Total)
}

func (suite *AnnotationsCounterTestSuite) TestCountMultiValue() {

	conceptUUID1 := uuid.New().String()
	expectedAnnotationsCount1 := 25
	suite.writeTestConceptWithAnnotations(conceptUUID1, 3, expectedAnnotationsCount1)

	conceptUUID2 := uuid.New().String()
	expectedAnnotationsCount2 := 10
	suite.writeTestConceptWithAnnotations(conceptUUID2, 1, expectedAnnotationsCount2)

	conceptUUID3 := uuid.New().String()
	expectedAnnotationsCount3 := 1234
	suite.writeTestConceptWithAnnotations(conceptUUID3, 5, expectedAnnotationsCount3)

	conceptUUID4 := uuid.New().String()
	expectedAnnotationsCount4 := 0
	suite.writeTestConceptWithAnnotations(conceptUUID4, 3, expectedAnnotationsCount4)

	uuids := []string{
		conceptUUID1,
		conceptUUID2,
		conceptUUID3,
		conceptUUID4,
	}

	ac := NewAnnotationsCounter(suite.driverPool, recentAnnotationsCountAge)
	counts, err := ac.Count(uuids)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), counts, 4)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount1), counts[conceptUUID1].Total)
	assert.Equal(suite.T(), int64(9), counts[conceptUUID1].Recent)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount2), counts[conceptUUID2].Total)
	assert.Equal(suite.T(), int64(4), counts[conceptUUID2].Recent)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount3), counts[conceptUUID3].Total)
	assert.Equal(suite.T(), int64(412), counts[conceptUUID3].Recent)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount4), counts[conceptUUID4].Total)
	assert.Equal(suite.T(), int64(0), counts[conceptUUID4].Recent)
}

func (suite *AnnotationsCounterTestSuite) TestCountWithMissingConcepts() {
	conceptUUID1 := uuid.New().String()
	expectedAnnotationsCount1 := 25
	suite.writeTestConceptWithAnnotations(conceptUUID1, 3, expectedAnnotationsCount1)

	conceptUUID2 := uuid.New().String()
	expectedAnnotationsCount2 := 10
	suite.writeTestConceptWithAnnotations(conceptUUID2, 1, expectedAnnotationsCount2)

	conceptUUID3 := uuid.New().String()
	conceptUUID4 := uuid.New().String()

	uuids := []string{
		conceptUUID1,
		conceptUUID2,
		conceptUUID3,
		conceptUUID4,
	}

	ac := NewAnnotationsCounter(suite.driverPool, recentAnnotationsCountAge)
	counts, err := ac.Count(uuids)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), counts, 2)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount1), counts[conceptUUID1].Total)
	assert.Equal(suite.T(), int64(expectedAnnotationsCount2), counts[conceptUUID2].Total)
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

func (suite *AnnotationsCounterTestSuite) writeTestConceptWithAnnotations(conceptPrefUUID string, equivalentConcepts, annotationCount int) {
	conn, err := suite.driverPool.OpenPool()
	require.NoError(suite.T(), err)
	defer conn.Close()

	//creation of canonical concept node
	_, err = conn.ExecNeo("CREATE (n:Concept{prefUUID: {prefUUID}})", map[string]interface{}{"prefUUID": conceptPrefUUID})
	require.NoError(suite.T(), err)

	for i := 0; i < equivalentConcepts; i++ {
		// create equivalent node
		equivalentConceptUUID := uuid.New().String()
		_, err = conn.ExecNeo("MATCH (n:Concept{prefUUID: {prefUUID}}) CREATE (n)<-[:EQUIVALENT_TO]-(x:Concept{uuid:{uuid}})",
			map[string]interface{}{"prefUUID": conceptPrefUUID, "uuid": equivalentConceptUUID})
		require.NoError(suite.T(), err)

		//create annotations
		subCount := annotationCount / equivalentConcepts
		if i == 0 {
			subCount += annotationCount % equivalentConcepts
		}

		now := int64(time.Now().Unix())
		for j := 0; j < subCount; j++ {
			isRecent := j%3 == 0

			var pubDate int64
			if isRecent {
				pubDate = now
			} else {
				pubDate = now - recentAnnotationsCountAge - 24*3600
			}
			_, err = conn.ExecNeo("MATCH (n:Concept{uuid: {uuid}}) CREATE (n)<-[:REL]-(c:Content{publishedDateEpoch: {pubDate}})",
				map[string]interface{}{"uuid": equivalentConceptUUID, "pubDate": pubDate})
			require.NoError(suite.T(), err)
		}

	}
}

func (suite *AnnotationsCounterTestSuite) cleanDB() {
	conn, err := suite.driverPool.OpenPool()
	require.NoError(suite.T(), err)
	defer conn.Close()

	//delete content
	_, err = conn.ExecNeo("MATCH (n:Content) DETACH DELETE n", nil)
	require.NoError(suite.T(), err)
	//delete concepts
	_, err = conn.ExecNeo("MATCH (n:Concept) DETACH DELETE n", nil)
	require.NoError(suite.T(), err)
}
