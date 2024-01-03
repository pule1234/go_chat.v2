package tools

import (
	"context"
	"github.com/gin-gonic/gin"
	uuid "github.com/satori/go.uuid"
	"github.com/tencentyun/cos-go-sdk-v5"
	"goim/config"
	"net/http"
	"net/url"
	"path"
)

func CosUpload(c *gin.Context) (string, error) {
	u, _ := url.Parse(config.CosBucket)
	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  config.TencentSecretID,
			SecretKey: config.TencentSecretKey,
		},
	})

	file, err := c.FormFile("file")
	if err != nil {
		return "", err
	}

	key := "cloud-disk/" + UUID() + path.Ext(file.Filename)

	src, err := file.Open()
	if err != nil {
		return "", err
	}

	defer src.Close()

	_, err = client.Object.Put(
		context.Background(), key, src, nil,
	)
	if err != nil {
		return "", err
	}

	return config.CosBucket + "/" + key, nil
}

func UUID() string {
	return uuid.NewV4().String()
}
