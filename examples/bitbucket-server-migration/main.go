package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/src-d/metadata-retrieval/github"
	"github.com/src-d/metadata-retrieval/github/store"

	"github.com/lwsanty/bitclient"
	"golang.org/x/oauth2"
	"gopkg.in/src-d/go-cli.v0"
	"gopkg.in/src-d/go-log.v1"
)

var app = cli.New("migrate", "0", "0", "github -> bitbucket server metadata migration")

type Config struct {
	cli.Command     `name:"migrate"`
	GithubRepoOwner string `long:"github-repo-owner" env:"GITHUB_REPO_OWNER" description:""`
	GithubRepoName  string `long:"github-repo-name" env:"GITHUB_REPO_NAME" description:""`

	BitBucketServerAddress    string `long:"bit-server-address" env:"BIT_SERVER_ADDRESS" description:""`
	BitBucketServerUser       string `long:"bit-server-user" env:"BIT_SERVER_USER" description:""`
	BitBucketServerPass       string `long:"bit-server-pass" env:"BIT_SERVER_PASS" description:""`
	BitBucketServerProjectKey string `long:"bit-server-project-key" env:"BIT_SERVER_PROJECT_KEY" description:""`
}

func main() {
	app.AddCommand(&Config{})
	app.RunMain()
}

func (c *Config) Execute(args []string) error {
	ctx := context.Background()
	githubClient := oauth2.NewClient(ctx, oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: os.Getenv("GITHUB_TOKEN")},
	))

	startGet := time.Now()
	m, err := github.GetMemStore(ctx, githubClient, c.GithubRepoOwner, c.GithubRepoName)
	if err != nil {
		return fmt.Errorf("failed to get mem store: %v", err)
	}
	log.Infof("GetMemStore: %v", time.Since(startGet))

	startMigrate := time.Now()
	defer func() {
		log.Infof("Migrate: %v", time.Since(startMigrate))
	}()
	return c.migrate(m)
}

func (c *Config) migrate(m *store.Mem) error {
	repo, ok := m.Repos[c.GithubRepoOwner][c.GithubRepoName]
	if !ok {
		return fmt.Errorf("failed to obtain repo %s", c.GithubRepoName)
	}

	bitClient := bitclient.NewBitClient(c.BitBucketServerAddress, c.BitBucketServerUser, c.BitBucketServerPass)
	for prId, pr := range repo.PRs {
		if pr.PullRequest.State != "OPEN" {
			continue
		}
		if err := c.migratePR(bitClient, prId, pr); err != nil {
			//return fmt.Errorf("failed to migrate PR %v: %v", prId, err)
			log.Errorf(err, "============> failed to migrate PR %v", prId)
		}
	}
	return nil
}

func (c *Config) migratePR(b *bitclient.BitClient, prId int, pr store.PullRequest) error {
	bitPRId, err := c.createPR(b, pr)
	if err != nil {
		return err
	}
	log.Infof("bitPRId: %v", bitPRId)

	_, err = c.createComments(b, bitPRId, pr)
	if err != nil {
		return err
	}

	return c.createReviewComments(b, bitPRId, pr)
}

func (c *Config) createReviewComments(b *bitclient.BitClient, prId int, pr store.PullRequest) error {
	reviews := pr.Reviews
	for _, review := range reviews {
		reviewResp, err := b.CreatePullRequestComment(c.BitBucketServerProjectKey,
			c.GithubRepoName,
			strconv.Itoa(prId),
			bitclient.CreatePullRequestCommentParams{
				// TODO: format
				Text: trim(fmt.Sprintf("%+v", review)),
			})
		if err != nil {
			return err
		}

		for _, cm := range review.Comments {
			_, err := b.CreatePullRequestComment(c.BitBucketServerProjectKey,
				c.GithubRepoName,
				strconv.Itoa(prId),
				bitclient.CreatePullRequestCommentParams{
					// TODO: format
					Text: trim(fmt.Sprintf("%+v", cm)),
					Parent: &bitclient.CreatePullRequestCommentParentParams{
						Id: reviewResp.Id,
					},
				})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Config) createComments(b *bitclient.BitClient, prId int, pr store.PullRequest) (map[string]int, error) {
	comments := pr.Comments
	result := make(map[string]int)

	for _, cm := range comments {
		resp, err := b.CreatePullRequestComment(c.BitBucketServerProjectKey,
			c.GithubRepoName,
			strconv.Itoa(prId),
			bitclient.CreatePullRequestCommentParams{
				// TODO: format
				Text: trim(fmt.Sprintf("%+v", cm)),
			})
		if err != nil {
			return nil, err
		}
		result[cm.Id] = resp.Id
	}

	return result, nil
}

func (c *Config) createPR(b *bitclient.BitClient, pr store.PullRequest) (int, error) {
	log.Infof("creating PR")

	var (
		repoKey = c.BitBucketServerProjectKey
		slug    = c.GithubRepoName
		// note: we do not support users now so all comments and PRs will be from default user
		user  = c.BitBucketServerUser
		gitPR = pr.PullRequest
	)

	// currently PRs are created in the same repo range
	prResp, err := b.CreatePullRequest(repoKey, slug, bitclient.CreatePullRequestParams{
		Title:       gitPR.Title,
		Description: gitPR.Body,
		FromRef: bitclient.BranchRef{
			Id: gitPR.HeadRef.Name,
			Repository: bitclient.Repository{
				Slug: slug,
				Project: bitclient.Project{
					Key: repoKey,
				},
			},
		},
		ToRef: bitclient.BranchRef{
			Id: gitPR.BaseRef.Name,
			Repository: bitclient.Repository{
				Slug: slug,
				Project: bitclient.Project{
					Key: repoKey,
				},
			},
		},
		Reviewers: []bitclient.Participant{
			{User: bitclient.User{Name: user}},
		},
		CloseSourceBranch: false,
	})
	if err != nil {
		return 0, err
	}

	return prResp.Id, nil
}

func trim(s string) string {
	if len(s) > 1000 {
		return s[0:999] + "..."
	}
	return s
}
