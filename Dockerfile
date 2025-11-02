FROM golang:1.25.3

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /app/GoStream ./cmd/GoStream

# Run the built binary
CMD ["/app/GoStream"]
