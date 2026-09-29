module AsyncRabbitMQ
  # A broker delivery tag plus the connection generation it was issued on.
  #
  # Delivery tags are scoped to a channel on a connection: after a reconnect
  # the broker starts numbering from 1 again. A handler that was still running
  # when the connection dropped would otherwise ack a tag from the old
  # connection against the new channel, where that number now belongs to a
  # different message (silent loss, or a whole range of them with
  # multiple: true) or to no message at all (a 406 that closes the channel).
  #
  # Channel#basic_ack, #basic_nack and #basic_reject compare the generation
  # carried here against the channel's current one and drop stale tags.
  #
  # It stands in for the Integer it wraps: it converts implicitly (#to_int),
  # compares and sorts against Integers, and hashes equal to them, so code
  # that treats a delivery tag as a number keeps working.
  class VersionedDeliveryTag
    include Comparable

    attr_reader :tag, :generation

    def initialize(tag, generation)
      @tag        = tag
      @generation = generation
    end

    # True when this tag was issued on an earlier connection than +current+.
    def stale?(current)
      @generation < current
    end

    def to_i
      @tag
    end
    alias to_int to_i

    def coerce(other)
      [other, @tag]
    end

    def <=>(other)
      @tag <=> (other.respond_to?(:to_i) ? other.to_i : other)
    end

    def ==(other)
      (@tag <=> (other.respond_to?(:to_i) ? other.to_i : other)) == 0
    end
    alias eql? ==

    def hash
      @tag.hash
    end

    def to_s
      @tag.to_s
    end

    def inspect
      "#<#{self.class.name} #{@tag} gen=#{@generation}>"
    end
  end
end
