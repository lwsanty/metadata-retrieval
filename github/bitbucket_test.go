package github

import (
	"context"
	"os"
	"testing"

	"github.com/src-d/metadata-retrieval/github/store"

	"github.com/lwsanty/bitclient"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

const (
	owner = "lwsanty"
	name  = "dgraph"
)

func TestName(t *testing.T) {
	client := bitclient.NewBitClient("http://localhost:7990", os.Getenv("NAME"), os.Getenv("PASS"))

	err := client.CreatePullRequest("JOH", "dgraph", bitclient.CreatePullRequestParams{
		Title:       "oneone",
		Description: "desc",
		FromRef: bitclient.BranchRef{
			Id: "harshil-goel/searchable-search",
			Repository: bitclient.Repository{
				Slug: "dgraph",
				Project: bitclient.Project{
					Key: "JOH",
				},
			},
		},
		ToRef: bitclient.BranchRef{
			Id: "master",
			Repository: bitclient.Repository{
				Slug: "dgraph",
				Project: bitclient.Project{
					Key: "JOH",
				},
			},
		},
		Reviewers: []bitclient.Participant{
			{User: bitclient.User{Name: "johnny"}},
		},
		CloseSourceBranch: false,
	})
	require.NoError(t, err)
}

//func CreatePullRequest(c *bitclient.BitClient, projectKey, repositorySlug string) error {
//	_, err := c.DoPost(
//		fmt.Sprintf("/projects/%s/repos/%s/pull-requests", projectKey, repositorySlug),
//		json.RawMessage([]byte(data)),
//		nil,
//	)
//
//	return err
//}

func getMemStore(t *testing.T, ctx context.Context) *store.Mem {
	githubClient := oauth2.NewClient(ctx, oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: os.Getenv("GITHUB_TOKEN")},
	))

	d, err := NewMemDownloader(githubClient)
	if err != nil {
		panic(err)
	}

	require.NoError(t, d.DownloadRepository(ctx, owner, name, 0))

	memStore, ok := d.storer.(*store.Mem)
	require.True(t, ok)

	return memStore
}

func trim(s string) string {
	if len(s) > 40 {
		return s[0:39] + "..."
	}

	return s
}

const data = `{
  "title": "put some title1",
  "description": "put some desc",
  "fromRef": {
    "id": "harshil-goel/searchable-search",
    "repository": {
      "slug": "dgraph",
      "name": null,
      "project": {
        "key": "JOH"
      }
    }
  },
  "toRef": {
    "id": "master",
    "repository": {
      "slug": "dgraph",
      "name": null,
      "project": {
        "key": "JOH"
      }
    }
  },
  "reviewers": [
    {
      "user": {
        "name": "johnny"
      }
    }
  ],
  "close_source_branch": false
}`
