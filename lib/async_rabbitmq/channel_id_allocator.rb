# frozen_string_literal: true

require "amq/bit_set"

module AsyncRabbitMQ
  # Hands out channel ids from 1 to +limit+ and takes them back when a channel
  # closes, so a process that opens a channel per unit of work never runs out.
  # Built on AMQ::BitSet from amq-protocol, the same structure under Bunny's
  # ChannelIdAllocator: finding the lowest free id is a word scan, not a walk
  # of the channel table. AMQ::IntAllocator itself is not used because it
  # cannot reserve a specific id, which Channel#reopen needs to take its old id
  # back after a broker-initiated close.
  class ChannelIdAllocator
    attr_reader :limit

    def initialize(limit)
      raise ArgumentError, "channel limit must be positive (got #{limit.inspect})" unless limit.to_i.positive?

      @limit = limit
      @bits  = AMQ::BitSet.new(limit) # bit i stands for channel id i + 1
    end

    # The lowest free id, or nil when all +limit+ ids are in use. BitSet
    # answers -1 when every word is full, and can point into the unused tail of
    # the last word, which is why both bounds are checked.
    def allocate
      index = @bits.next_clear_bit
      return nil if index.nil? || index.negative? || index >= @limit

      @bits.set(index)
      index + 1
    end

    # Take a specific id. Returns false if it is already in use.
    def reserve(id)
      return false unless id.between?(1, @limit)
      return false if @bits.get(id - 1)

      @bits.set(id - 1)
      true
    end

    def release(id)
      @bits.unset(id - 1) if id.between?(1, @limit)
      nil
    end

    def allocated?(id)
      id.between?(1, @limit) && @bits.get(id - 1)
    end

    def reset
      @bits.clear
      nil
    end
  end
end
